import os
import pickle
import tensorflow as tf

from datetime import datetime

from config import Config
from utils import TFRecordWriter

flags = tf.app.flags
flags.DEFINE_string('process_datetime', '2017030100', 'The datetime to be processed')
FLAGS = flags.FLAGS


def main():

    config = Config()
    process_datetime = datetime.strptime(FLAGS.process_datetime, '%Y%m%d%H')

    df_campaign = pickle.load(open(config.df_campaign_path, 'r'))

    year = process_datetime.year
    month = process_datetime.month
    day = process_datetime.day
    hour = process_datetime.hour

    log_folder_path = os.path.join(config.raw_data_folder_path,
                                   str(year),
                                   str(month).zfill(2),
                                   str(day).zfill(2),
                                   str(hour).zfill(2))
    log_file_list = os.listdir(log_folder_path)
    log_file_path_list = [os.path.join(log_folder_path, file_name) for file_name in log_file_list]
    tfrecord_file_name = str(year) + str(month).zfill(2) + str(day).zfill(2) + str(hour).zfill(2) + '.tfrecord'
    tfrecord_file_path = os.path.join(config.tfrecord_folder_path, tfrecord_file_name)
    print("{} log files on {}".format(len(log_file_list), log_folder_path))

    tfrecord_writer = TFRecordWriter(log_file_path_list,
                                     tfrecord_file_path,
                                     config.log_filter_dict,
                                     df_campaign,
                                     config.features_type_dict,
                                     'default')
    tfrecord_writer.write_tfrecord()

if __name__ == '__main__':
    main()
