__author__ = 'mborodin'

import sys
import logging
from PyCool import cool

FOLDER_NAME = '/TRT/Calib/PID_vector'
TAG_NAME = 'TRTCalibPID_vector-000-01'

logging.basicConfig(level=logging.DEBUG)
_logger = logging.getLogger('createPIDTool')





def store_PID_to_folder(db,values_list):
        """
        Function to create folder FOLDER_NAME in db with channels and input data from values_list with tag TAG_NAME
        :param db: Open cool db
        :param values_list: list of tuples ('channel name', valus array)

        """
        spec = cool.RecordSpecification()
        spec.extend('array_value',cool.StorageType.Float)
        _logger.info('Create folder %s' % FOLDER_NAME)
        folder_spec = cool.FolderSpecification(cool.FolderVersioning.MULTI_VERSION,
                                           spec,
                                           cool.PayloadMode.VECTORPAYLOAD)
        folder = db.createFolder(FOLDER_NAME, folder_spec,
                               ' <timeStamp>time</timeStamp><addrHeader><address_header service_type="71" clid="55403898"/></addrHeader><typeName>CondAttrListVec</typeName>',
                               True)

        for index, channel_name in enumerate(values_list):
            folder.createChannel(index,channel_name)
            print '%i - %s' % (index, channel_name[0])
        folder.setupStorageBuffer()
        for index, channel_values in enumerate(values_list):
            _logger.info('Store data from %s' % channel_values[0])
            values = channel_values[1]
            vector = cool.IRecordVector()
            for value in values:
                data = cool.PyCool.Helpers.IRecordPtr(spec)
                data.get()['array_value'] = value
                vector.push_back(data)
            folder.storeObject(0,cool.ValidityKeyMax, vector, index, TAG_NAME )
        folder.flushStorageBuffer()


def read_PID_txt(input_file_name):
        """
        Read data from input txt file in awful format
        :param input_file_name: input file path
        """
        output_list=[]
        with open(input_file_name,'r') as input_file:
                for line in input_file:
                        line = line.replace(' ','').replace("\n",'')
                        if line and (line.find('#')==-1):
                                elements = line.split(',')

                                if len(elements) > 5:
                                    #Why?!
                                    barrel_or_endcap = elements[2].replace('EndcapWheels','EW')
                                    electrons = elements[3].replace('Electrons','Elecs')
                                    output_list.append(((elements[1]+'_'+barrel_or_endcap+'_'+electrons),map(float,elements[8:])))
        return output_list


def main():

        if len(sys.argv) != 4:
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
        folder = store_PID_to_folder(db)
        store_PID_to_folder(folder,values_dict)
        db.closeDatabase()

if __name__=='__main__':
        main()