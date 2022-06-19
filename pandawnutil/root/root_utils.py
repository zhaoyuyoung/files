import re
import subprocess


# get ROOT version and setup strings
def get_version_setup_string(root_ver, cmt_config):
    if not root_ver:
        return '', ''
    rootVer = root_ver
    if rootVer == 'recommended':
        rootCVMFS = rootVer
    else:
        if rootVer.count('.') != 2:
            rootVer += ".00"
        # CVMFS version format
        if not cmt_config:
            cmt_config = None

            com = "export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase; "\
                  "source $ATLAS_LOCAL_ROOT_BASE/user/atlasLocalSetup.sh --quiet; "\
                  "showVersions root"
            out = subprocess.check_output(com, shell=True, universal_newlines=True)
            for l in out.split('\n'):
                if rootVer + '-' in l:
                    cmt_config = re.sub(rootVer + '-', '', l.split()[1])
            if cmt_config:
                print ("Use {} for ROOT {} found in ALRB although --cmtConfig was not specified".format(cmt_config,
                                                                                                        rootVer))
            else:
                print ("Use x86_64-centos7-gcc8-opt for ROOT by default when --cmtConfig is unset")
                cmt_config = 'x86_64-centos7-gcc8-opt'
        rootCVMFS = rootVer + '-' + cmt_config
    # setup string
    tmpSetupEnvStr = ("export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase; "
                      "source $ATLAS_LOCAL_ROOT_BASE/user/atlasLocalSetup.sh --quiet; "
                      "source $ATLAS_LOCAL_ROOT_BASE/packageSetups/atlasLocalROOTSetup.sh "
                      "--rootVersion={0} --skipConfirm; ").format(rootCVMFS)
    return rootCVMFS, tmpSetupEnvStr
