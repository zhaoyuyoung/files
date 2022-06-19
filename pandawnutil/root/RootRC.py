import os
import re

rootRcFileName = '.rootrc'

# customize .rootrc
def customizeRootRC(dirName,numProofWokers):
    rootRcFile = '%s/%s' % (dirName,rootRcFileName)
    changes = {'ProofLite.Workers':numProofWokers}
    rootConfigs = []
    # read config if .rootrc is there
    if os.path.exists(rootRcFile):
        rcFile = open(rootRcFile)
        for line in rcFile:
            line = line.strip()
            # look for parms to be changed
            toOverwite = False
            for changeKey in changes.keys():
                patt = '^%s( |=|:)' % changeKey
                if re.search(patt,line):
                    toOverwite = True
                    break
            # append
            if not toOverwite:
                rootConfigs.append(line)
        rcFile.close()
    # rename old .rootrc
    try:
        os.rename(rootRcFile,rootRcFile+'.old')
    except:
        pass
    # generate new .rootrc
    rcFile = open(rootRcFile,'w')
    for line in rootConfigs:
        rcFile.write(line+'\n')
    for changeKey in changes:
        changeValue = changes[changeKey]
        rcFile.write('%s: %s\n' % (changeKey,changeValue))
    rcFile.close()
    return


# dump .rootrc
def dumpRootRC(dirName):
    rootRcFile = '%s/%s' % (dirName,rootRcFileName)
    dumpStr = ''
    try:
        rcFile = open(rootRcFile)
        for line in rcFile:
            dumpStr += line
        rcFile.close()
    except:
        pass
    dumpStr += '\n'
    return dumpStr

