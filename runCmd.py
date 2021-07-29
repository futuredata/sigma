# tart python vm : pipenv shell
# install dependences: pipenv install --dev

import subprocess
import sys, getopt
import os

def default_arg_msg():
    print('runCmd.py -t <type> -f <folder> -c <config> -C <backend_config> -n <output_filename> -q <is_csv>')

if __name__ == '__main__':
    folder = ''
    parsetype = ''
    config = ''
    backend_config = ''
    output_filename = ''
    is_csv = ''
    
    try:
        opts, args = getopt.getopt(sys.argv[1:],"t:f:c:C:n:q:",["type=","folder=", "config=", "backend_config=", "output_filename=", "is_csv"])
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
            elif opt in ("-n", "--output_filename"):
                output_filename = arg
            elif opt in ("-q", "--is_csv"):
                is_csv = arg
    else:
        default_arg_msg()
        sys.exit()
    
    print('Load folder is ', folder)
    print('Parse type  is ', parsetype)  
    print('Config  is ', config)
    print('Config Backend  is ', backend_config)
    print('Output filename  is ', output_filename)
    print('Is CSV ', is_csv)
    
    onlyfiles = [os.path.join(r,file) for r,d,f in os.walk(folder) for file in f]
    
    loadfile = './ignore-filename.txt'
    ignorelist = [];

    
    for line in open(loadfile,'r'):
        if line != "" :
            ignorelist.append(os.path.normpath(line.replace("\n", "")))
    print(ignorelist)  
    
    if(output_filename != ''):
        file = open(output_filename, "a")
        if(is_csv == '1'):
            file.write("\"InfoId\",\"Tenant\",\"Type\",\"Name\",\"Description\",\"FalsePositiveCheck\",\"Analysis\",\"Recommendation\",\"Severity\",\"Rule\",\"IsExp\",\"EvtSt\",\"EvtObj\",\"EvtCon\",\"EvtAct\",\"OutObj\",\"OutCon\",\"OutPro\",\"Status\",\"EvtTime\",\"Suppression\",\"SMStatus\",\"ThresholdType\",\"BucketSize\",\"ThresholdFirstValue\",\"ThresholdSecondValue\",\"TmStatus\",\"DrillDownQuery\"\n");
        for f in onlyfiles:
            if os.path.normpath(f) in ignorelist:
                continue
            print("\n")
            batcmd="python ./tools/sigmac -t " + parsetype + " -c " + config + " -C " + backend_config + " " + f
            print('======= ' + batcmd)
            result = subprocess.check_output(batcmd, shell=True, universal_newlines=False, encoding=False)
            if(is_csv != '1'):
                file.write('======' + f + '\n')
            file.write(result.decode("utf-8"))
            print(result.decode("utf-8") , end = '', flush=True)
        file.close()
    else:
        for f in onlyfiles:
            if os.path.normpath(f) in ignorelist:
                continue
            print("\n")
            batcmd="python ./tools/sigmac -t " + parsetype + " -c " + config + " -C " + backend_config + " " + f
            print('======= ' + batcmd)
            result = subprocess.check_output(batcmd, shell=True, universal_newlines=False, encoding=False)
            print(result.decode("utf-8") , end = '', flush=True)

