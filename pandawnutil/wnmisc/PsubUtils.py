import re
import sys
from rucio.client import Client as RucioClient
from pandawnutil.wnlogger import PLogger
try:
    long
except NameError:
    long = int
# have to reset logger since DQ2 tweaks logger
PLogger.resetLogger()

EC_Failed = 255
EC_Config = 100


# convert GoodRunListXML to datasets
def convertGoodRunListXMLtoDS(tmpLog,goodRunListXML,goodRunDataType='',goodRunProdStep='',
                              goodRunListDS='',verbose=False):
    tmpLog.info('trying to convert GoodRunListXML to a list of datasets')  
    # return for failure
    failedRet = False,'',[]
    # import pyAMI
    try:
        import pyAMI.utils
        import pyAMI.client
        import pyAMI.atlas.api as AtlasAPI
    except:
        errType,errValue = sys.exc_info()[:2]
        print ("%s %s" % (errType,errValue))
        tmpLog.error('cannot import pyAMI module')
        return failedRet
    # read XML
    try:
        gl_xml = open(goodRunListXML)
    except:
        tmpLog.error('cannot open %s' % goodRunListXML)
        return failedRet
    # parse XML to get run/lumi
    runLumiMap = {}
    import xml.dom.minidom
    rootDOM = xml.dom.minidom.parse(goodRunListXML)
    for tmpLumiBlock in rootDOM.getElementsByTagName('LumiBlockCollection'):
        for tmpRunNode in tmpLumiBlock.getElementsByTagName('Run'):
            tmpRunNum  = long(tmpRunNode.firstChild.data)
            for tmpLBRange in tmpLumiBlock.getElementsByTagName('LBRange'):
                tmpLBStart = long(tmpLBRange.getAttribute('Start'))
                tmpLBEnd   = long(tmpLBRange.getAttribute('End'))
                # append
                if tmpRunNum not in runLumiMap:
                    runLumiMap[tmpRunNum] = []
                runLumiMap[tmpRunNum].append((tmpLBStart,tmpLBEnd))
    kwargs = dict()
    kwargs['run_number'] = [str(x) for x in runLumiMap.keys()]
    kwargs['ami_status'] = 'VALID'
    if goodRunDataType != '':
        kwargs['type'] = goodRunDataType
    if goodRunProdStep != '':    
        kwargs['stream'] = goodRunProdStep
    if verbose:
        tmpLog.debug(kwargs)
    # convert for wildcard
    goodRunListDS = goodRunListDS.replace('*','.*')
    # list of datasets
    if goodRunListDS == '':
        goodRunListDS = []
    else:
        goodRunListDS = goodRunListDS.split(',')
    # execute
    try:
        amiclient = pyAMI.client.Client('atlas')
        amiOutDict = pyAMI.utils.smart_execute(amiclient, 'datasets', [], None, None, None, False, **kwargs).get_rows()
    except:
        errType,errValue = sys.exc_info()[:2]
        tmpLog.error("%s %s" % (errType,errValue))
        tmpLog.error('pyAMI failed')
        return failedRet
    # get dataset map
    if verbose:
        tmpLog.debug(amiOutDict)
    # parse
    rucioclient = RucioClient()
    datasetListFromAMI = []
    runDsMap = dict()
    for tmpVal in amiOutDict:
        if 'ldn' in tmpVal:
            dsName = str(tmpVal['ldn'])
            # check dataset names
            if goodRunListDS == []:    
                matchFlag = True
            else:
                matchFlag = False
                for tmpPatt in goodRunListDS:
                    if re.search(tmpPatt,dsName) != None:
                        matchFlag = True
            if not matchFlag:
                continue
            # get metadata
            try:
                tmpLog.debug("getting metadata for %s" % dsName)
                scope,dsn = extract_scope(dsName)
                meta =  rucioclient.get_metadata(scope, dsn)
                if meta['did_type'] == 'COLLECTION':
                    dsName += '/'
                datasetListFromAMI.append(dsName)
                tmpRunNum = meta['run_number']
                tmpLog.debug("run number : %s" % tmpRunNum)
                if tmpRunNum not in runDsMap:
                    runDsMap[tmpRunNum] = set()
                runDsMap[tmpRunNum].add(dsName)
            except:
                tmpLog.debug("failed to get metadata")
    # make string
    datasets = ''
    filesStr = []
    for tmpRunNum in runLumiMap.keys():
        for dsName in runDsMap[tmpRunNum]:
            # get files in the dataset
            tmpFilesStr = []
            tmpLFNList = []
            scope,dsn = extract_scope(dsName)
            for x in rucioclient.list_files(scope, dsn, long=True):
                tmpLFN = str(x['name'])
                LBstart_LFN = x['lumiblocknr']
                if LBstart_LFN is not None:
                    LBend_LFN = LBstart_LFN
                else:
                    # extract LBs from LFN
                    tmpItems = tmpLFN.split('.')
                    # short format
                    if len(tmpItems) < 7:
                        tmpFilesStr.append(tmpLFN)
                        continue
                    tmpLBmatch = re.search('_lb(\d+)-lb(\d+)',tmpLFN)
                    # _lbXXX-lbYYY found
                    if tmpLBmatch is not None:
                        LBstart_LFN = long(tmpLBmatch.group(1))
                        LBend_LFN   = long(tmpLBmatch.group(2))
                    else:
                        # try ._lbXYZ.
                        tmpLBmatch = re.search('\._lb(\d+)\.',tmpLFN)
                        if tmpLBmatch is not None:
                            LBstart_LFN = long(tmpLBmatch.group(1))
                            LBend_LFN   = LBstart_LFN
                        else:
                            tmpFilesStr.append(tmpLFN)
                            continue
                # check range
                inRange = False
                for LBstartXML,LBendXML in runLumiMap[tmpRunNum]:
                    if (LBstart_LFN >= LBstartXML and LBstart_LFN <= LBendXML) or \
                       (LBend_LFN >= LBstartXML and LBend_LFN <= LBendXML) or \
                       (LBstart_LFN >= LBstartXML and LBend_LFN <= LBendXML) or \
                       (LBstart_LFN <= LBstartXML and LBend_LFN >= LBendXML):
                        inRange = True
                        break
                if inRange:
                    tmpFilesStr.append(tmpLFN)
            # check if files are found
            if tmpFilesStr == []:
                tmpLog.warning('found no files with corresponding LBs in %s' % dsName)
            else:
                datasets += '%s,' % dsName
                filesStr += tmpFilesStr    
    datasets = datasets[:-1]
    if verbose:
        tmpLog.debug('converted to DS:%s LFN:%s' % (datasets,str(filesStr)))
    # return        
    return True,datasets,filesStr


# extract scope and dataset name
def extract_scope(dsn):
    if ':' in dsn:
        return dsn.split(':')[:2]
    scope = dsn.split('.')[0]
    if dsn.startswith('user') or dsn.startswith('group'):
        scope = ".".join(dsn.split('.')[0:2])
    return scope,dsn
