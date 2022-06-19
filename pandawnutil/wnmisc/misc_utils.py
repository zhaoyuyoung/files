import os
import json
import time
import zlib
import datetime
import shutil
try:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError
    from urllib.parse import urlencode
    from http.client import IncompleteRead
except ImportError:
    from urllib import urlencode
    from urllib2 import urlopen, HTTPError, Request
    from httplib import IncompleteRead
import ssl
import subprocess
import multiprocessing
import uuid
from pandawnutil.wnlogger import PLogger


# internal env variable to record the execution home directory name
ENV_HOME = 'TRF_EXEC_HOME_DIR'

# env variable of payload input directory
ENV_WORK_DIR = 'PAYLOAD_INPUT_DIR'


# add user job metadata
def add_user_job_metadata(userJobMetadata='userJobMetadata.json'):
    # check user metadata
    if not os.path.exists(userJobMetadata):
        return
    # size check
    if os.stat(userJobMetadata).st_size > 1024*1024:
        print ("WARNING : user job metadata is too large > 1MB")
        return
    merged_dict = dict()
    # get user metadata
    print ("\n=== user metadata in {0} ===".format(userJobMetadata))
    try:
        with open(userJobMetadata) as f:
            print(f.read())
            f.seek(0)
            tmp_dict = json.load(f)
    except Exception:
        print ("ERROR : user job metadata is corrupted")
        return
    # check job report
    jobReport = 'jobReport.json'
    if os.path.exists(jobReport):
        with open(jobReport) as f:
            merged_dict = json.load(f)
        os.rename(jobReport, jobReport+'.org')
    # add
    merged_dict.setdefault('user_job_metadata', dict())
    merged_dict['user_job_metadata'].update(tmp_dict)
    # version number
    if 'reportVersion' not in merged_dict:
        merged_dict['reportVersion'] = '1.0.0'
    # dump
    with open(jobReport, 'w') as f:
        json.dump(merged_dict, f)


# make a tarball for log files in sub dirs
def make_log_tarball_in_sub_dirs(tar_file_path):
    try:
        patt = ['*.log', 'log*']
        patt_str = '-o '.join(['-name "{0}" '.format(i) for i in patt])
        # command
        com = 'find . -mindepth 2 '
        com += r'-type f \( ' + patt_str + r'\) -print0 '
        com += '| '
        com += 'tar -z '
        com += '-c -f {0} --null -T -'.format(tar_file_path)
        commands_get_status_output(com)
        if os.path.exists(tar_file_path) and os.path.getsize(tar_file_path) == 0:
            os.remove(tar_file_path)
    except Exception:
        pass

# make http body with multipart/form-data encoding
def encode_multipart_form_data(name, file_name):
    boundary = uuid.uuid4().hex.upper()
    items = []
    items.append(("--{0}".format(boundary)).encode())
    items.append(('Content-Disposition: form-data; name="{0}"; filename="{1}"'.format(name, file_name)).encode())
    items.append('Content-Type: application/octet-stream'.encode())
    items.append(''.encode())
    with open(file_name, 'rb') as f:
        items.append(f.read())
    items.append(('--{0}--'.format(boundary)).encode())
    items.append(''.encode())
    body = '\r\n'.encode().join(items)
    content_type = "multipart/form-data; boundary={0}".format(boundary)
    return body, content_type


# get file via http
def get_file_via_http(base_url='', file_name='', full_url='', data=None, headers=None,
                      certfile=None, keyfile=None, method=None, filename_to_upload=None,
                      force_access=False):
    if full_url == '':
        url = "%s/cache/%s" % (base_url, file_name)
    else:
        url = full_url
        if file_name == '':
            file_name = url.split('/')[-1]
    tmpMsg = "--- Access to %s" % url
    if headers is None:
        headers = {}
    if filename_to_upload is not None:
        data, content_type = encode_multipart_form_data('file', filename_to_upload)
        headers.update({'Content-Type': content_type, 'Content-Length': len(data)})
    else:
        if data is not None:
            tmpMsg += ' {0}'.format(str(data))
        if data is not None:
            data = urlencode(data).encode()
    print (tmpMsg)
    # the file already exists in the current directory
    if not force_access:
        if os.path.exists(file_name):
            print ("skip since the file already exists in the current directory")
            return True, None
        # the file exists in the home directory or payload working directory
        for tmpEnv in [ENV_HOME, ENV_WORK_DIR]:
            if tmpEnv in os.environ:
                fileInHome = os.path.join(os.environ[tmpEnv], file_name)
                if os.path.exists(fileInHome):
                    # make symlink
                    os.symlink(fileInHome, file_name)
                    print ("skip since the file is available in {0}".format(os.environ[tmpEnv]))
                    return True, None
    isOK = False
    errStr = None
    for i in range(3):
        try:
            if method is None:
                req = Request(url, data=data, headers=headers)
            else:
                try:
                    req = Request(url, data=data, headers=headers, method=method)
                except Exception:
                    # for python 2
                    class MyRequest(Request):
                        def get_method(self, *args, **kwargs):
                            return method
                    req = MyRequest(url, data=data, headers=headers)
            try:
                context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
                if certfile is not None:
                    context.load_cert_chain(certfile, keyfile)
            except Exception:
                # for old python
                res = urlopen(req)
            else:
                res = urlopen(req, context=context)
            with open(file_name, 'wb') as f:
                full_read = res.read()
                f.write(full_read)
            # size check
            try:
                cont_size = res.headers.get('content-length', None)
            except Exception:
                cont_size = None
            if cont_size is None:
                print ('skip size check since content-length is missing')
            else:
                cont_size = int(cont_size)
                act_size = os.stat(file_name).st_size
                print ('content-length={0} actual-size={1}'.format(cont_size, act_size))
                if cont_size != act_size:
                    raise IncompleteRead(full_read, cont_size-act_size)
            isOK = True
            break
        except HTTPError as e:
            errStr = 'HTTP code: {0} - Reason: {1}'.format(e.code, e.reason)
            # doesn't exist
            if e.code == 404:
                break
        except Exception as e:
            errStr = str(e)
            time.sleep(30)
    if not isOK:
        return False, "Failed with {0}".format(errStr)
    if not os.path.exists(file_name):
        return False, 'Unable to fetch %s from web' % file_name
    print ("succeeded")
    return True, None


# propagate missing sandbox error to the pilot
def propagate_missing_sandbox_error():
    print ('ERROR: unable to fetch source tarball from web')


# replacement for commands
def commands_get_status_output(com):
    data = ''
    try:
        # not to use check_output for python 2.6
        # data = subprocess.check_output(com, shell=True, universal_newlines=True, stderr=subprocess.STDOUT)
        p = subprocess.Popen(com, shell=True, universal_newlines=True, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        data, unused_err = p.communicate()
        retcode = p.poll()
        if retcode:
            ex = subprocess.CalledProcessError(retcode, com)
            raise ex
        status = 0
    except subprocess.CalledProcessError as ex:
        # commented out for python 2.6
        # data = ex.output
        status = ex.returncode
    if data[-1:] == '\n':
        data = data[:-1]
    return status, data


# record current exec directory
def record_exec_directory():
    currentDir = os.getcwd()
    os.environ[ENV_HOME] = currentDir
    print ("--- Running in %s ---" % currentDir)
    return currentDir


# get HPO sample
def get_hpo_sample(idds_url, task_id, sample_id, certfile, keyfile):
    url = os.path.join(idds_url, 'idds', 'hpo', str(task_id), 'null', str(sample_id), 'null', 'null')
    file_name = '__tmp_get.out'
    s, o = get_file_via_http(file_name=file_name, full_url=url, certfile=certfile, keyfile=keyfile)
    if not s:
        return False, o
    try:
        with open(file_name) as f:
            print ('')
            print(f.read())
            f.seek(0)
            tmp_dict = json.load(f)
            for i in tmp_dict:
                if i['id'] == sample_id:
                    return True, i
    except Exception as e:
        errStr = "failed to get the sample (ID={0}) : {1}".format(sample_id, str(e))
        return False, errStr
    return False, "cannot get the sample (ID={0}) since it is unavailable".format(sample_id)


# update HPO sample
def update_hpo_sample(idds_url, task_id, sample_id, loss, certfile, keyfile):
    url = os.path.join(idds_url, 'idds', 'hpo', str(task_id), 'null', str(sample_id), str(loss))
    file_name = '__tmp_update.out'
    s, o = get_file_via_http(file_name=file_name, full_url=url, method='PUT',
                             certfile=certfile, keyfile=keyfile)
    if not s:
        return False, o
    try:
        with open(file_name) as f:
            print ('')
            print(f.read())
            f.seek(0)
            tmp_dict = json.load(f)
            if tmp_dict['status'] == 0:
                return True, None
    except Exception as e:
        errStr = "failed to update the sample (ID={0}) : {1}".format(sample_id, str(e))
        return False, errStr
    return False, "cannot update the sample (ID={0}) since status is missing".format(sample_id)


# update events
def update_events(panda_url, event_id, status, certfile, keyfile):
    updateEventFileName = '__update_event.json'
    commands_get_status_output('rm -rf %s' % updateEventFileName)
    # update event
    data = dict()
    data['eventRangeID'] = event_id
    data['eventStatus'] = status
    data = {'eventRanges': json.dumps([data]), 'version': 1}
    url = panda_url + '/server/panda/updateEventRanges'
    tmpStat, tmpOut = get_file_via_http(file_name=updateEventFileName, full_url=url, data=data,
                                        headers={'Accept': 'application/json'},
                                        certfile=certfile, keyfile=keyfile)
    print ('\nstatus={0} out={1}'.format(tmpStat, tmpOut))
    if tmpStat:
        with open(updateEventFileName) as f:
            print (f.read())


# make a tarball when there are new files
def make_tarball_for_fresh_files(files_to_check, output_file, last_check_time, max_size, tmp_log):
    try:
        files_to_check = files_to_check.split(',')
        makeTarball = False
        for path in files_to_check:
            if os.path.isfile(path):
                mtime = datetime.datetime.utcfromtimestamp(os.path.getmtime(path))
                tmp_log.debug('{0} mtime:{1} last_check:{2}'.format(path, mtime, last_check_time))
                # found a new file
                if mtime >= last_check_time:
                    makeTarball = True
                    break
            else:
                for root, dirs, files in os.walk(path):
                    for name in files:
                        sub_path = os.path.join(root, name)
                        mtime = datetime.datetime.utcfromtimestamp(os.path.getmtime(sub_path))
                        tmp_log.debug('{0} mtime:{1} last_check:{2}'.format(sub_path, mtime, last_check_time))
                        # found a new file
                        if mtime >= last_check_time:
                            makeTarball = True
                            break
                    if makeTarball:
                        break
            if makeTarball:
                break
        # no new files
        if not makeTarball:
            return False
        # make a tarball
        tmp_output_file = '{0}.tmp'.format(output_file)
        st, out = commands_get_status_output('rm -rf {0}; tar cvfz {0} {1}'.format(tmp_output_file,
                                                                                   ' '.join(files_to_check)))
        if st != 0 or not os.path.exists(tmp_output_file):
            tmp_log.error("failed to tar files to {0}: {1}".format(output_file, out))
            return False
        # size check
        tmpSize = os.stat(tmp_output_file).st_size // 1024 // 1024
        if tmpSize > max_size:
            tmp_log.error("tar file is too large {0} > {1} MB".format(tmpSize, max_size))
            return False
        shutil.move(tmp_output_file, output_file)
        tmp_log.info("new tarball with size: {0} MB".format(tmpSize))
        return True
    except Exception as e:
        tmp_log.error('!!!! failed to make a tarball due to {0}'.format(str(e)))
        return False

# parse harvester events json
def parse_harvester_events_json(panda_id, source, destination):
    # look for source path
    source_path = None
    if os.path.exists(source):
        source_path = source
    else:
        # look in the home directory or payload working directory
        for tmpEnv in [ENV_HOME, ENV_WORK_DIR]:
            if tmpEnv in os.environ:
                tmpPath = os.path.join(os.environ[tmpEnv], source)
                if os.path.exists(tmpPath):
                    source_path = tmpPath
                    break
    # not found
    if source_path is None:
        return False, 'source={0} not found'.format(source)
    panda_id = str(panda_id)
    # parse
    try:
        print (source_path)
        with open(source_path) as fi:
            print(fi.read())
            fi.seek(0)
            data = json.load(fi)
            if panda_id not in data:
                return False, 'PandaID={0} not found in json'.format(panda_id)
            # dump
            with open(destination, 'w') as fo:
                if data[panda_id]:
                    json.dump(data[panda_id][0], fo)
                else:
                    # dummy
                    json.dump({}, fo)
        return True, None
    except Exception as e:
        return False, 'failed to parse harvester json: {0}'.format(str(e))


# class to periodically upload checkpoint files
class CheckPointUploader:
    def __init__(self, task_id, panda_id, sample_id, files_to_check, check_interval, panda_url, certfile, keyfile,
                 verbose, offline_mode=False, dump_file=None):
        self.task_id = task_id
        self.panda_id = panda_id
        self.sample_id = sample_id
        self.output_filename = '{0}_{1}'.format(task_id, sample_id)
        self.files_to_check = files_to_check
        self.check_interval = check_interval
        self.panda_url = panda_url
        self.certfile = certfile
        self.keyfile = keyfile
        self.body = None
        self.verbose = verbose
        self.offline_mode = offline_mode
        self.dump_file = dump_file
        self.log_filename = 'log.checkpoint_uploader'
        self.tmpLog = PLogger.getPandaLogger(log_file_name=self.log_filename)

    # main to launch a sub-process
    def start(self):
        print ('=== launching checkpoint uploader ===\n')
        self.body = multiprocessing.Process(target=self._run)
        self.body.daemon = True
        self.body.start()
        print ("up and running. logging in {0}".format(self.log_filename))

    # terminator
    def terminate(self):
        print ('=== terminating checkpoint uploader ===')
        self.body.terminate()
        self.body.join()
        # delete checkpoint file
        print ('terminated')
        print ('')

    # cleanup
    def cleanup(self):
        if self.offline_mode:
            return
        print ('=== trying to cleanup checkpoint ===')
        data = dict()
        data['task_id'] = self.task_id
        data['sub_id'] = self.sample_id
        tmpDump = '__deleteCheckPoint.json'
        url = self.panda_url + '/server/panda/delete_checkpoint'

        tmpStat, tmpOut = get_file_via_http(file_name=tmpDump, full_url=url, data=data,
                                            certfile=self.certfile, keyfile=self.keyfile,
                                            force_access=True)
        if not tmpStat:
            print ("ERROR: failed to cleanup checkpoint " + tmpOut)
        with open(tmpDump) as f:
            print (f.read())
        print ('')

    # calculate adler32
    def calc_adler32(self, file_name):
        val = 1
        blockSize = 32 * 1024 * 1024
        with open(file_name, 'rb') as fp:
            while True:
                data = fp.read(blockSize)
                if not data:
                    break
                val = zlib.adler32(data, val)
        if val < 0:
            val += 2 ** 32
        return hex(val)[2:10].zfill(8).lower()

    # real main
    def _run(self):
        last_check_time = datetime.datetime.utcnow()
        tmpDump = '__putCheckPoint.json'
        url = self.panda_url + '/server/panda/put_checkpoint'
        while True:
            # check fresh files
            self.tmpLog.debug('check fresh files')
            is_new = make_tarball_for_fresh_files(self.files_to_check, self.output_filename, last_check_time,
                                                  100, self.tmpLog)
            last_check_time = datetime.datetime.utcnow()
            if self.verbose:
                self.tmpLog.debug('is new? {0}'.format(is_new))
            if is_new:
                if not self.offline_mode:
                    # upload
                    self.tmpLog.info('uploading new checkpoint')
                    tmpStat, tmpOut = get_file_via_http(file_name=tmpDump, full_url=url,
                                                        filename_to_upload=self.output_filename, force_access=True,
                                                        certfile=self.certfile, keyfile=self.keyfile)
                    self.tmpLog.info('status={0} out={1}'.format(tmpStat, tmpOut))
                    if tmpStat:
                        with open(tmpDump) as f:
                            self.tmpLog.info(f.read())
                else:
                    self.tmpLog.info('dump to json')
                    if os.path.exists(self.dump_file):
                        self.tmpLog.info('skip since the file exists')
                    else:
                        with open(self.dump_file, 'w') as f:
                            path = os.path.join(os.getcwd(), self.output_filename)
                            json.dump({str(self.panda_id): [{"guid": str(uuid.uuid4()),
                                                             "path": path,
                                                             "fsize": os.stat(path).st_size,
                                                             "type": "checkpoint",
                                                             "chksum": self.calc_adler32(path)}
                                                            ]}, f)
                        self.tmpLog.info('done')
            if self.verbose:
                self.tmpLog.debug('go to sleep for {0} min'.format(self.check_interval))
            time.sleep(self.check_interval * 60)
