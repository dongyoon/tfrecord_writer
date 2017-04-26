import os


class Config(object):

    def __init__(self, data_config='default'):

        # Constants
        self.MIN_AGE = 0
        self.MAX_AGE = 100
        self.GAP_AGE = self.MAX_AGE - self.MIN_AGE
        self.MIN_DIFF_DAYS = 0
        self.MAX_DIFF_DAYS = 730
        self.GAP_DIFF_DAYS = self.MAX_DIFF_DAYS - self.MIN_DIFF_DAYS
        self.MIN_HOUR = 0
        self.MAX_HOUR = 23
        self.GAP_HOUR = self.MAX_HOUR - self.MIN_HOUR

        # Configuration for data source
        self.init_date = '20170301'
        self.end_date = '20170303'

        if data_config == 'test':
            self.init_date = '20170401'
            self.end_date = '20170408'

        self.raw_data_folder_path = './data/raw'  # raw data
        self.campaign_folder_path = './data/campaign'  # import path
        self.tfrecord_folder_path = './data/tfrecords'  # export path

        self.df_campaign_path = os.path.join(self.campaign_folder_path, 'df_campaign.p')
        self.contain_image = False

        # Configuration for query log data
        self.log_filter_dict = {
            'country': 'KR'
        }

        # Configuration to write tfrecord
        self.features_type_dict = {
            'sex': 'float64',
            'age': 'float64',
            'publisher_int': 'int64',
            'categories_int': 'int64',
            'categories_int_len': 'int64',
            'title_pos_int': 'int64',
            'title_pos_int_len': 'int64',
            'platform': 'float64',
            'diff_days': 'float64',
            'weekday': 'float64',
            'hour': 'float64',
            'click': 'int64'
        }
