import os
import os.path
import uuid
import subprocess
from pandawnutil.wnmisc.misc_utils import commands_get_status_output

if os.path.isabs(__file__):
    modFullName = __file__
else:
    modFullName = os.getcwd() + '/' + __file__
modFullPath = os.path.dirname(modFullName)    


class RunTracer:
    # constructor
    def __init__(self,debugFlag=False):
        self.wrapperName = 'wrapper'
        self.archOptMap  = [('32bit', '-m32', 'lib'), ('64bit', '-m64', 'lib64')]
        try:
            import platform
            if platform.processor() in ['aarch64']:
                self.archOptMap = [('32bit', None, 'lib'), ('64bit', None, 'lib64')]
        except Exception:
            pass
        self.libBaseDir  = None
        self.logName     = ''
        self.debugFlag   = debugFlag


    # make wrapper
    def make(self, verbose=False):
        print ("\n===== make PandaTracer =====")
        # create lib base dir
        if self.debugFlag:
            self.libBaseDir = os.getcwd()
        else:
            self.libBaseDir = os.getcwd() + '/' + str(uuid.uuid4())
            commands_get_status_output('rm -rf %s' % self.libBaseDir)
            os.makedirs(self.libBaseDir)
        # set output filename
        self.logName = self.libBaseDir + '/pandatracerlog.txt'
        outH = open('outfilename.h','w')
        outH.write('const char *pandatracer_outfilename = "%s";\n' % self.logName)
        outH.write("const char *pandatracer_sofilename = \"%s/$LIB/%s.so\";\n" % \
                   (self.libBaseDir,self.wrapperName))
        outH.close()
        # make lib and lib64
        for arcStr, archOpt, archLib in self.archOptMap:
            print("  {0} making with opt={1} in {2}".format(arcStr, archOpt, archLib))
            if archOpt:
                step_base = 'gcc {0} '.format(archOpt)
            else:
                step_base = 'gcc '
            step1 = step_base + '-I. -fPIC -c -Wall %s/%s.c -o %s.o' % \
                    (modFullPath, self.wrapperName, self.wrapperName)
            step2 = step_base + '-shared %s.o -ldl -lstdc++ -o %s/%s/%s.so' % \
                    (self.wrapperName,self.libBaseDir,archLib,self.wrapperName)
            step_d = step_base + '-shared -fpic -o %s/%s/%s.so -xc /dev/null ' % \
                    (self.libBaseDir,archLib,self.wrapperName)
            # makedir
            try:
                os.makedirs(self.libBaseDir+'/'+archLib)
            except:
                pass
            isFailed = False
            # make
            if verbose:
                print("    com for make: {0}".format(step1))
            p = subprocess.Popen(step1.split(), stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
            out, err = p.communicate()
            if p.returncode != 0:
                if verbose:
                    print ("    failed with {0}".format(err))
                isFailed = True
            else:
                if verbose:
                    print("    com for so: {0}".format(step2))
                p = subprocess.Popen(step2.split(), stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
                out, err = p.communicate()
                if p.returncode != 0:
                    if verbose:
                        print("    failed with {0}".format(err))
                    isFailed = True
            # make dummy if failed
            if isFailed:
                if verbose:
                    print("    com for dummy: {0}".format(step_d))
                p = subprocess.Popen(step_d.split(), stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
                out, err = p.communicate()
                if p.returncode != 0:
                    if verbose:
                        print("    failed with {0}".format(err))
                    print ("  %s is not supported" % arcStr)
                else:
                    print ("  %s uses dummy" % arcStr)
            else:
                print ("  %s succeeded" % arcStr)
            if verbose:
                print("")
        # log name
        commands_get_status_output('touch %s' % self.getLogName())
        print ("Log location -> %s" % self.getLogName())
        # return
        return

                                
    # get env var
    def getEnvVar(self):
        envStr  = ''
        envStr += "export PANDA_PRELOAD=%s/'$LIB'/%s.so${LD_PRELOAD:+:$LD_PRELOAD}; " % \
                  (self.libBaseDir,self.wrapperName)
        envStr += "export LD_PRELOAD=%s/'$LIB'/%s.so${LD_PRELOAD:+:$LD_PRELOAD}; " % \
                  (self.libBaseDir,self.wrapperName)
        return envStr


    # get log name
    def getLogName(self):
        return self.logName
