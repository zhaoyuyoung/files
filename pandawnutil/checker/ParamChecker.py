import re


# check sourceURL
def checkSourceHost(sourceURL):
    try:
        # get hostname
        sourceHost = sourceURL.replace('/',' ').split()[1]
        # remove port
        sourceHost = re.sub(':\d+$','',sourceHost)
        if not sourceHost in ['voatlas177.cern.ch','voatlas178.cern.ch',
                              'voatlas57.cern.ch','voatlas58.cern.ch','voatlas59.cern.ch',
                              'voatlas248.cern.ch','voatlas249.cern.ch','voatlas250.cern.ch',
                              'voatlas251.cern.ch','voatlas252.cern.ch','voatlas253.cern.ch',
                              'voatlas254.cern.ch','voatlas255.cern.ch']:
            print ('ERROR: un-trusted host %s for input sandbox' % sourceHost)
            return False
        # OK
        return True
    except:
        print ('ERROR: cannot get HOST from sourceURL=%s' % sourceURL)
        return False
