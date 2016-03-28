__author__ = 'mborodin'

__author__ = 'mborodin'

import sys
import logging
from PyCool import cool

FOLDER_NAME = '/TRT/Calib/ToT/ToTVectors'
TAG_NAME = 'TRTCalibToTToTVectors-000-02'

logging.basicConfig(level=logging.DEBUG)
_logger = logging.getLogger('createToTTool')





def store_ToT_to_folder(db,values_list):
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
                               ' <timeStamp>run-lumi</timeStamp><addrHeader><address_header service_type="71" clid="55403898"/></addrHeader><typeName>CondAttrListVec</typeName>',
                               True)

        for index, channel_name in enumerate(values_list):
            folder.createChannel(index,channel_name[0])
            #print '%i - %s' % (index, channel_name[0])
        folder.setupStorageBuffer()
        for index, channel_values in enumerate(values_list):
            #_logger.info('Store data from %s' % channel_values[0])
            values = channel_values[1]
            vector = cool.IRecordVector()
            for value in values:
                data = cool.PyCool.Helpers.IRecordPtr(spec)
                data.get()['array_value'] = value
                vector.push_back(data)
            folder.storeObject(0,cool.ValidityKeyMax, vector, index, TAG_NAME )
        folder.flushStorageBuffer()
        spec = cool.RecordSpecification()
        ToT_values = {}
        ToT_values['paraL_dEdx_p1'] = -0.192682
        ToT_values['paraL_dEdx_p2'] = -6.95687
        ToT_values['paraL_dEdx_p3'] = -0.974757
        ToT_values['paraL_dEdx_p4'] = 1.27633
        ToT_values['paraL_dEdx_p5'] = -0.014986

        ToT_values['para_dEdx_p1'] = -0.206938
        ToT_values['para_dEdx_p2'] = -7.55895
        ToT_values['para_dEdx_p3'] = -0.991186
        ToT_values['para_dEdx_p4'] = 1.26495
        ToT_values['para_dEdx_p5'] = -0.0043035
        ToT_values['norm_offset_data'] = 0.07
        ToT_values['norm_slope_tot'] =  0.0035
        ToT_values['norm_slope_totl'] =  0.0031
        ToT_values['norm_offset_tot'] =  0.930
        ToT_values['norm_offset_totl'] =  0.975

        for field_name in ToT_values.keys():
            spec.extend(field_name,cool.StorageType.Float)
        ToT_values['norm_nzero'] = 8
        spec.extend('norm_nzero',cool.StorageType.Int32)

        _logger.info('Create folder /TRT/Calib/ToT/ToTValue' )
        folder_spec = cool.FolderSpecification(cool.FolderVersioning.MULTI_VERSION,
                                           spec,
                                           cool.PayloadMode.SEPARATEPAYLOAD)
        folder = db.createFolder('/TRT/Calib/ToT/ToTValue', folder_spec,
                               ' <timeStamp>run-lumi</timeStamp><addrHeader><address_header service_type="71" clid="1238547719"/></addrHeader><typeName>CondAttrListCollection</typeName>',
                               True)
        data =  cool.Record( spec )
        for field_name in ToT_values.keys():
            data[field_name] = ToT_values[field_name]
        folder.storeObject(0,cool.ValidityKeyMax, data, 0, 'TRTCalibToTToTValue-000-02' )
        folder.storeObject(0,cool.ValidityKeyMax, data, 1, 'TRTCalibToTToTValue-000-02' )
        folder.storeObject(0,cool.ValidityKeyMax, data, 2, 'TRTCalibToTToTValue-000-02' )
        folder.flushStorageBuffer()


def read_ToT(is_data=True):
        output_list=[]
        # dict_name_lists = ['resolution','resolution_e','para_long_corrRZ_MC','para_short_corrRZ_MC','para_end_corrRZ_MC','para_long_corrRZL_MC',
        #               'para_short_corrRZL_MC','para_end_corrRZL_MC']
        # dict_name_lists = ['resolution','resolution_e','para_long_corrRZ','para_short_corrRZ','para_end_corrRZ','para_long_corrRZL_DATA',
        #               'para_short_corrRZL_DATA','para_end_corrRZL_DATA']

        full_dict_names = ['para_end_corrRZL_DATA_Ar', 'para_end_corrRZL_DATA_Xe', 'para_end_corrRZL_MC_Ar', 'para_end_corrRZL_MC_Xe', 'para_end_corrRZ_Ar', 'para_end_corrRZ_MC_Ar', 'para_end_corrRZ_MC_Xe', 'para_end_corrRZ_Xe', 'para_end_mimicToXe_DATA_Ar', 'para_end_mimicToXe_DATA_Xe', 'para_end_mimicToXe_MC_Ar', 'para_end_mimicToXe_MC_Xe', 'para_long_corrRZL_DATA_Ar', 'para_long_corrRZL_DATA_Xe', 'para_long_corrRZL_MC_Ar', 'para_long_corrRZL_MC_Xe', 'para_long_corrRZ_Ar', 'para_long_corrRZ_MC_Ar', 'para_long_corrRZ_MC_Xe', 'para_long_corrRZ_Xe', 'para_long_mimicToXe_DATA_Ar', 'para_long_mimicToXe_DATA_Xe', 'para_long_mimicToXe_MC_Ar', 'para_long_mimicToXe_MC_Xe', 'para_short_corrRZL_DATA_Ar', 'para_short_corrRZL_DATA_Xe', 'para_short_corrRZL_MC_Ar', 'para_short_corrRZL_MC_Xe', 'para_short_corrRZ_Ar', 'para_short_corrRZ_MC_Ar', 'para_short_corrRZ_MC_Xe', 'para_short_corrRZ_Xe', 'para_short_mimicToXe_DATA_Ar', 'para_short_mimicToXe_DATA_Xe', 'para_short_mimicToXe_MC_Ar', 'para_short_mimicToXe_MC_Xe', 'resolution_Ar', 'resolution_Xe', 'resolution_e_Ar', 'resolution_e_Xe']
        if is_data:
            dict_name_lists = [x for x in full_dict_names if (('MC' not in x) and ('_Xe' in x))]
            dict_name_lists += [x for x in full_dict_names if (('MC' not in x) and ('_Ar' in x))]
            dict_name_lists += [x.replace('_Ar','_Kr') for x in full_dict_names if (('MC' not in x) and ('_Ar' in x))]
        else:
            dict_name_lists = [x for x in full_dict_names if ((('MC' in x) or ('resolution')) and ('_Xe' in x))]
            dict_name_lists += [x for x in full_dict_names if ((('MC' in x) or ('resolution')) and ('_Ar' in x))]
            dict_name_lists += [x.replace('_Ar','_Kr') for x in full_dict_names if ((('MC' in x) or ('resolution')) and ('_Ar' in x))]
        print ','.join(['"'+x+'"' for x in dict_name_lists])
        from fullEDx import FullEdx
        for current_dict in dict_name_lists:
            output_list.append((current_dict.replace('_Kr','_Ar'),getattr(FullEdx,current_dict.replace('_Kr','_Ar'))))
        return output_list


def main():

        if len(sys.argv) != 3:
            print "Usage: %s  connection_string data|mc" % sys.argv[0]
            return -1
        if sys.argv[2] !='mc':
            values_dict = read_ToT(False)
        else:
            values_dict = read_ToT(True)
        connect_string = sys.argv[1]
        _logger.info("data: %s" % values_dict)
        try:
            dbSvc = cool.DatabaseSvcFactory.databaseService()
            _logger.debug('dropping database %s' % connect_string)
            dbSvc.dropDatabase( connect_string )
            _logger.debug('creating database')
            db = dbSvc.createDatabase( connect_string )
        except Exception,e:
            _logger.error("Problem with database: %s" % e)
            return -1
        store_ToT_to_folder(db, values_dict)
        db.closeDatabase()

if __name__=='__main__':
        main()