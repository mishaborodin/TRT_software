__author__ = 'mborodin'

import sys
import logging
from PyCool import cool

FOLDER_NAME = '/TRT/Calib/PID_vector'

_logger = logging.getLogger('createPIDTool')


def create_PID_folder(db):
    """
    Create folder in db for PID
    :param db:
    :return: folder object
    """
    spec = cool.RecordSpecification()
    spec.extend('array_value',cool.StorageType.Float)
    folder_spec = cool.FolderSpecification(cool.FolderVersioning.MULTI_VERSION,
                                       spec,
                                       cool.PayloadMode.VECTORPAYLOAD,
                                       True)
    return db.createFolder(FOLDER_NAME, folder_spec)


def store_PID_to_folder(folder,values_dict):
        """
        Read data from input txt file in awful format
        :param input_file_name: input file path
        """
        CHANNEL_NAME = [('')]
        return 1

def read_PID_txt(input_file_name):
        """
        Read data from input txt file in awful format
        :param input_file_name: input file path
        """
        output_dict={}
        with open(input_file_name,'r') as input_file:
                for line in input_file:
                        line = line.replace(' ','').replace("\n",'')
                        if line and (line.find('#')==-1):
                                elements = line.split(',')
                                #Why?!
                                output_dict[elements[0]+'-'+elements[2]+'-'+elements[3]] = map(float,elements[8:])
        return output_dict


def main():
        if len(sys.argv) != 3:
            print "Usage: %s input_file connection_string" % sys.argv[0]
            return -1
        values_dict = read_PID_txt(sys.argv[1])
        connect_string = sys.argv[2]
        _logger.info("data from file: %s" % values_dict)
        try:
            dbSvc = cool.DatabaseSvcFactory.databaseService()
            _logger.debug('dropping database %s' % connect_string)
            dbSvc.dropDatabase( connect_string )
            _logger.debug('creating database')
            db = dbSvc.createDatabase( connect_string )
        except Exception,e:
            _logger.error("Problem with database: %s" % e)
            return -1
        folder = create_PID_folder(db)
        store_PID_to_folder(folder,values_dict)
        db.closeDatabase()

if __name__=='__main__':
        main()