__author__ = 'mborodin'

import sys
import logging
from PyCool import cool

FOLDER_NAME = '/TRT/ToT/ToTVectors'

logging.basicConfig(level=logging.DEBUG)
_logger = logging.getLogger('readToT')



def  load_data(db, tag):
    folder = db.getFolder(FOLDER_NAME)
    objects = folder.browseObjects(cool.ValidityKeyMin, cool.ValidityKeyMax, cool.ChannelSelection.all(), tag)
    for item in objects:
        print "IOV [%d, %d[ c:%s " % ( item.since(), item.until(), item.channelId() )
        for payload in item.payloadIterator():
            print payload,
        print ''


def main():

        if len(sys.argv) != 3:
            print "Usage: %s  db tag" % sys.argv[0]
            return -1
        tag = sys.argv[2]
        try:
            connect_string = "oracle://ATLAS_COOLPROD;schema=ATLAS_COOLOFL_TRT;dbname={dbname}".format(dbname=sys.argv[1])
            dbSvc = cool.DatabaseSvcFactory.databaseService()
            _logger.debug('Open database connect_string')
            db = dbSvc.openDatabase( connect_string )
        except Exception,e:
            _logger.error("Problem with database: %s" % e)
            return -1
        data_dict = load_data(db,tag)
        db.closeDatabase()

if __name__=='__main__':
        main()