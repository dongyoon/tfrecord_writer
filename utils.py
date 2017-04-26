import tensorflow as tf
import gzip
import json
import numpy as np
import re

from datetime import datetime
from timeit import default_timer as timer

from config import Config


class TFRecordWriter(object):

    def __init__(self, file_path_list, tfrecord_file_path, log_filter_dict,
                 df_campaign, features_type_dict, config_type):

        config = Config(config_type)

        self.file_path_list = file_path_list
        self.tfrecord_file_path = tfrecord_file_path
        self.log_filter_dict = log_filter_dict
        self.df_campaign = df_campaign
        self.features_type_dict = features_type_dict
        self.writer = tf.python_io.TFRecordWriter(self.tfrecord_file_path)
        self.click_file_list = sorted([f for f in file_path_list if re.search('click', f)], reverse=False)
        self.impression_file_list = sorted([f for f in file_path_list if re.search('impression', f)], reverse=False)

        self.GAP_AGE = config.GAP_AGE
        self.GAP_DIFF_DAYS = config.GAP_DIFF_DAYS
        self.GAP_HOUR = config.GAP_HOUR

    def _paste_campaign_row(self, row):
        dict_campaign = self.df_campaign[self.df_campaign['id'] == row['campaign_id']].to_dict(orient='records')[0]
        row.update(dict_campaign)
        return row

    def _convert_to_numeric(self, row):
        row['sex'] = [0. if 'sex' not in row else 1. if row['sex'] == 'M' else -1.]
        row['age'] = [0. if 'age' not in row
                      else (2./self.GAP_AGE*(datetime.now().year - int(row['year_of_birth']))-1)]
        row['diff_days'] = [2./self.GAP_DIFF_DAYS*(row['time']-row['created_at']).days-1]
        row['weekday'] = [2./6*row['time'].weekday()-1]
        row['hour'] = [2./self.GAP_HOUR*row['time'].hour-1]
        row['click'] = [1 if row['message'] == 'click' else 0]
        return row

    @staticmethod
    def _write_feature(value, feature_type):

        if feature_type == 'int64':
            return tf.train.Feature(int64_list=tf.train.Int64List(value=value))
        elif feature_type == 'float64':
            return tf.train.Feature(float_list=tf.train.FloatList(value=value))
        elif feature_type == 'bytes':
            return tf.train.Feature(bytes_list=tf.train.BytesList(value=value))
        else:
            print('wrong feature type')
            return None

    def _write_tfrecord_example(self, row):
        features_dict_writer = {}
        for feature, feature_type in self.features_type_dict.items():
            value = row[feature]
            if not type(value) in [list, np.ndarray]:
                value = [value]
            features_dict_writer[feature] = self._write_feature(value, feature_type)
        example = tf.train.Example(features=tf.train.Features(feature=features_dict_writer))
        return example

    def _write_row(self, row):
        if all(k in row for k in self.log_filter_dict.keys()):
            if all(row[k] == v for k, v in self.log_filter_dict.items()):
                row = self._paste_campaign_row(row)
                row = self._convert_to_numeric(row)
                example = self._write_tfrecord_example(row)
                self.writer.write(example.SerializeToString())

    def write_tfrecord(self):

        click_list = []
        for file_path in self.click_file_list:
            with gzip.open(file_path, 'r') as rf:
                content = rf.read()
                click_list.extend(content.decode('utf-8').split('\n')[:-1])

        process_time = datetime(2000, 1, 1, 0, 0)
        click_idx = 0

        for file_path in self.impression_file_list:
            with gzip.open(file_path, 'r') as rf:
                content = rf.read()
                impression_list = content.decode('utf-8').split('\n')

                start = timer()

                for row_imp in impression_list[:-1]:
                    row_imp = json.loads(row_imp)
                    row_imp['time'] = datetime.strptime(row_imp['time'], '%Y-%m-%dT%H:%M:%S')

                    if row_imp['time'] > process_time:
                        process_time = row_imp['time']
                        if click_idx < len(click_list):
                            row_click = json.loads(click_list[click_idx])
                            row_click['time'] = datetime.strptime(row_click['time'], '%Y-%m-%dT%H:%M:%S')

                            while row_click['time'] <= process_time:
                                self._write_row(row_click)
                                click_idx += 1
                                row_click = json.loads(click_list[click_idx])
                                row_click['time'] = datetime.strptime(row_click['time'], '%Y-%m-%dT%H:%M:%S')

                    self._write_row(row_imp)

                end = timer()
                print("{} rows processed by {}".format(len(impression_list), end-start))

        self.writer.close()
