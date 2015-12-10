__author__ = 'mborodin'

import sys
import logging
from PyCool import cool

FOLDER_NAME = '/TRT/Calib/ToT/ToTVectors'

CHANNEL_NAME_LIST = ['resolution','resolution_e','para_long_corrRZ','para_short_corrRZ','para_end_corrRZ','para_long_corrRZL_DATA',
              'para_short_corrRZL_DATA','para_end_corrRZL_DATA']

logging.basicConfig(level=logging.DEBUG)
_logger = logging.getLogger('readToT')



def load_data(db, tag):
    """

    :param db: open cool db connection
    :param tag: tag name
    :return: list of tuples (dict name, dict values) which read from last IOV FOLDER_NAME vector folder
    """
    folder = db.getFolder(FOLDER_NAME)
    objects = folder.browseObjects(cool.ValidityKeyMin, cool.ValidityKeyMax, cool.ChannelSelection.all(), tag)
    dicts_from_cool = {}
    for item in objects:
        #print "IOV [%d, %d[ c:%s " % ( item.since(), item.until(), CHANNEL_NAME_LIST[int(item.channelId())] )
        dicts_from_cool[CHANNEL_NAME_LIST[int(item.channelId())]] = []
        for payload in item.payloadIterator():
            #print payload.get()['array_value'],
            dicts_from_cool[CHANNEL_NAME_LIST[int(item.channelId())]].append(payload.get()['array_value'])
        #print ''
    return_list = []
    for channel_name in CHANNEL_NAME_LIST:
        return_list.append((channel_name,dicts_from_cool[channel_name]))
    return return_list


def readToTFolder(db_name,tag_name):
        try:
            connect_string = "oracle://ATLAS_COOLPROD;schema=ATLAS_COOLOFL_TRT;dbname={dbname}".format(dbname=db_name)
            dbSvc = cool.DatabaseSvcFactory.databaseService()
            _logger.debug('Open database %s' % connect_string)
            db = dbSvc.openDatabase( connect_string )
        except Exception,e:
            _logger.error("Problem with database: %s" % e)
            raise e
        data_dict = load_data(db,tag_name)
        print data_dict

        db.closeDatabase()

def main():

        if len(sys.argv) != 3:
            print "Usage: %s  db tag" % sys.argv[0]
            return -1
        print readToTFolder(sys.argv[1],sys.argv[2])


if __name__=='__main__':
        main()