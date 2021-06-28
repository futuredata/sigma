# tart python vm : pipenv shell
# install dependences: pipenv install --dev

import subprocess
import sys, getopt
import os

def default_arg_msg():
    print('runCmd.py -t <type> -f <folder> -c <config> -C <backend_config>')

if __name__ == '__main__':
    folder = ''
    parsetype = ''
    config = ''
    backend_config = ''
    
    try:
        opts, args = getopt.getopt(sys.argv[1:],"t:f:c:C:",["type=","folder=", "config=", "backend_config="])
    except getopt.GetoptError:
        default_arg_msg()
        sys.exit(2)
    if len(opts) >= 3 :
        for opt, arg in opts:
            if opt in ("-t", "--type"):
                parsetype = arg
            elif opt in ("-f", "--folder"):
                folder = arg
            elif opt in ("-c", "--config"):
                config = arg
            elif opt in ("-C", "--backend_config"):
                backend_config = arg
    else:
        default_arg_msg()
        sys.exit()
    
    print('Load folder is "', folder)
    print('Parse type  is "', parsetype)  
    print('Config  is "', config)
    print('Config Backend  is "', backend_config)
    
    onlyfiles = [os.path.join(r,file) for r,d,f in os.walk(folder) for file in f]
    
    loadfile = './ignore-filename.txt'
    ignorelist = [];
    
    for line in open(loadfile,'r'):
        if line != "" :
            ignorelist.append(line.replace("\n", ""))
    print(ignorelist)  
    
    for f in onlyfiles:
        if f in ignorelist:
            continue
        print("\n")
        batcmd="python ./tools/sigmac -t " + parsetype + " " + f + " -c " + config + " -C " + backend_config
        print('======= ' + batcmd)
        result = subprocess.check_output(batcmd, shell=True, universal_newlines=False, encoding=False)
        print(result.decode("utf-8") , end = '', flush=True)

