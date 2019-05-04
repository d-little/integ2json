""" Converts an Intersystem's Cache database integrity file from text to JSON. """

""" 
To do: 
    - Handle gzip files using https://docs.python.org/3/library/gzip.html
    - Profile summary
        - Total Size 
            - Currently we sum the total length of each global, however this 
                rarely adds up to the actual running times
            - It would be better to calculate based off the start+end time 
                of the integ file
    - Deidentify the data
        - 'Global1' should be the same for every database when using 'generic' option
    - Test on operating systems than other AIX
    - Test on files with different errors
"""

import sys
from pathlib import Path
import argparse
import re
import json
import tempfile
import errno
import hashlib
import zipfile

def main(args):
    """ The whole dang thing. """    
    # Path-anise our args.files:
    listof_integ_files = list(Path(x).resolve() for x in args.files)        
    # Make sure each of the supplied integ files are 'OK'
    cleaned_listof_integ_files = list(listof_integ_files) 
    # ^^^ I do this because we remove files form the list, and removing objects from an iterobject as you iter is bad.  It's probably better to add?
    for integ_file in listof_integ_files:
        # Make sure that the top line matches something like: 
        #   Cache Database Integrity Check on 11/04/2018 at 19:00:01
        # This seems like a TERRIBLE way of ensuring it's a Cache integrity file but I have only bad ideas.
        if is_compressed(integ_file):
            # OK. Compressed file.  
            #   I'll need to refactor a lot of code to get this to work without reading it into memory.. to come.
            # 1) Set up a temp dir and find an integ file we extracted
            # 2) Remove this integ file from the integlist.  Add the one we found. 
            print("Skipping compressed file {} because we don't currently support it.".format(integ_file))
            cleaned_listof_integ_files.remove(integ_file)
            continue

        # Make sure integfile is an actual integrity file
        if not is_integfile(integ_file):
            print("Skipping file {} because it's not an integrity file.".format(integ_file))
            #remove it from the listof_integ_files
            cleaned_listof_integ_files.remove(integ_file)
            continue
        
        # Make sure outfile doesnt exist
        if args.singlefile:
            outfile = args.outdir / "integ2json.json"
            if outfile.exists():
                sys.exit("Default single-output file exists: {}".format(outfile))
        else:
            outfile = Path(integ_file).with_suffix('.json')
            if outfile.exists():
                print("Skipping file {} because its outfile exists: {}".format(integ_file, outfile))
                #remove it from the listof_integ_files
                cleaned_listof_integ_files.remove(integ_file)
                continue
        
    # integ_json is the big ol' dict where we store the JSON
    integ_json = {}
    listof_integ_files = list(cleaned_listof_integ_files)
    for integ_file in listof_integ_files:
        if_str = str(integ_file.name)
        integ_json[if_str] = {}
        integ_json[if_str].update(deal_with_integfile(integ_file))
        if not args.singlefile:
            if args.deidentify:
                integ_json=deidentify_json(str(args.deidentify), integ_json)
            outfile = integ_file.with_suffix('.json')
            if args.outdir:
                outfile = args.outdir / outfile.name

            print(f'Converting {integ_file} to {outfile}')
            output_to_file(integ_json=integ_json, outfile=outfile, beautify=args.beautify)
            integ_json = {} # reset integ_json back to empty
    
    if args.singlefile:
        if args.deidentify:
            integ_json = deidentify_json(str(args.deidentify), integ_json)
        outfile = args.outdir / Path("integ2json.json")
        if args.outdir:
            outfile = args.outdir / outfile.name
        print('Outputting to {}'.format(outfile))
        output_to_file(integ_json, outfile, args.beautify)
    #-------------
    return

def is_integfile(integ_file:Path)->bool:
    """ Checks to see if supplied integ_file is an Intersystems Cache integrity file.  
    Returns True/False. """
    try:
        with open(integ_file,'r') as fp:
            topLine = fp.readline().strip().split()
            fp.close()
    except Exception as e:
        if e.errno == errno.ENOENT:
            sys.exit("Integrity file does not exist: {}".format(integ_file))
        else:
            sys.exit("Failed to check status of integrity file: {}".format(integ_file))
    return ' '.join(topLine[0:5]) == 'Cache Database Integrity Check on'


def is_compressed(file: Path) -> bool:
    ''' Return True if file is a supported compressed file, else False '''
    # I would like to replace this with python_magic in the future, check the magic number instead.
    # But I would also like to use the base libraries... what to do, what to do
    valid_suffix = [ '.zip', '.gz' ]
    filetype = file.suffix
    if filetype not in set(valid_suffix):
        # Not compressed, but exists
        return False
    return True

def decompress(compressedfile:Path, destination:Path) -> bool:
    ''' Decompresses the given compressedfile and stores files in destination. 
    Returns True if file successfully decompressed, else False.
    Ideally you should be using a temporary path, but you could extract anywhere you want.'''
    '''
    eg: 
      - /path/to/pbuttons.zip returns: /temporary/path/pbuttons.html
      - c:\\mydir\\pbuttons.html.gz returns: c:\\temporary\\path\\pbuttons.html
      - c:\\mydir\\uncompressed_pbuttons.html returns c:\\mydir\\uncompressed_pbuttons.html
    '''
    try:
        if not compressedfile.exists:
            raise ValueError("Passed compressedfile does not exist: {}", compressedfile)
        filetype = compressedfile.suffix
        if filetype == '.zip':
            with open(compressedfile, "rb") as f:
                zf = zipfile.ZipFile(f)
                zf.extractall(destination)
        elif filetype == '.gz':
            import gzip ## move to top of script, here for now for testing
            import shutil ## ^^^
            # We dont want to uncompress the pbuttons in memory that's tooo much memory. Instead we tream it out.
            # https://codereview.stackexchange.com/questions/156005/improving-gzip-function-for-huge-files
            tgtpath = destination / Path(compressedfile.stem)
            with gzip.open(compressedfile, 'rb') as f_in, open(tgtpath, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        else:
            raise Exception('Unhandled compressed filetype.  This should not occur.')
    except OSError as e:
        sys.exit("Could not process compressed pButtons file because: {}".format(str(e)))
    return True


def deidentify_json(deIdentifyMethod:str, integ_json:{}):
    """ Takes given JSON, de-identifies any private data, such as Database and Global Names, returns JSON. """
    ####
    ## If we are deIdentifying data, there are two main ways: hash and 'tags' ?
    #   - hash replaces them with a hashed version of the string.
    #    - Tags replaces the values with an iterator, like database1, global1 etc
    # Protecting GDPR Personal Data with Pseudonymization:
    #   https://www.elastic.co/blog/gdpr-personal-data-pseudonymization-part-1
    if deIdentifyMethod == "generic":
        i_if=0
    for integfile in integ_json.keys(): 
        # profile section first
        if deIdentifyMethod == "hash":
            integfileDeIdent = "integfile_" + hash_string(integfile)
        elif deIdentifyMethod == "generic":
            integfileDeIdent = "integfile_" + str(i_if)
            i_if+=1
            i_db=0
            
        integ_json[integfileDeIdent] = integ_json.pop(integfile)
        sysName = "SystemName_" + \
            hash_string(integ_json[integfileDeIdent]["profile"]["System_Name"])
        sysInst = "SystemInstance_" + \
            hash_string(integ_json[integfileDeIdent]["profile"]["System_Instance"])
        integ_json[integfileDeIdent]["profile"]["System_Name"] = sysName
        integ_json[integfileDeIdent]["profile"]["System_Instance"] = sysInst 
        for database in integ_json[integfileDeIdent]["databases"].keys():
            if deIdentifyMethod == "hash":
                databaseDeIdent = "database_" + hash_string(database)
            else:
                databaseDeIdent = "database_" + str(i_db)
                i_db+=1
                i_gb=0
            integ_json[integfileDeIdent]["databases"][databaseDeIdent] = \
                integ_json[integfileDeIdent]["databases"].pop(database)
            for glbl in integ_json[integfileDeIdent]["databases"][databaseDeIdent]["globals"].keys():
                if deIdentifyMethod == "hash":
                    glblDeIdent = "global_" + hash_string(glbl)
                else:
                    glblDeIdent = "global_" + str(i_gb)
                    i_gb+=1
                integ_json[integfileDeIdent]["databases"][databaseDeIdent]["globals"][glblDeIdent] = \
                    integ_json[integfileDeIdent]["databases"][databaseDeIdent]["globals"].pop(glbl)
    
    return integ_json

    
def hash_string(string:str)->str:
    """ Returns a hash of the given string. """
    # Salt Generation and open source software 
    #https://stackoverflow.com/questions/1645161/salt-generation-and-open-source-software/1645190#1645190
    # md5 and sha256 have collission attacks, but that's ok for this context.
    #  Collisions (probably) don't leak data.
    if "md5" in hashlib.algorithms_guaranteed:
        hash="md5" 
    elif "sha512" in hashlib.algorithms_guaranteed:
        hash="sha512" #
    elif "sha256" in hashlib.algorithms_guaranteed:
        hash="sha256" 
    else:
        sys.exit("No acceptable hash in hashlib.")
    
    salt="nA8UvBjWa8" # salt from random.org password generator
    h = hashlib.new(hash)
    h.update(string+salt)
    return str(h.hexdigest())

    
def output_to_file(integ_json:dict, outfile:Path, beautify:bool)->None:
    """ Outputs JSON to a file. """
    with open(outfile, 'w') as file:
        if beautify:
            json.dump(integ_json, file, sort_keys=True, indent=4) 
            # nb: beautify significantly increases the size of the output file
        else:
            json.dump(integ_json, file)
    #print("Output JSON to " + outfile)
    
    
def deal_with_integfile(integ_file:Path)->{}:
    """ Processes entire IntegFile, return JSONs the whole thing. """
    # We use `with` to open the file because: 
    #  https://stackabuse.com/read-a-file-line-by-line-in-python/
    with open(integ_file,'r') as fp:
        """ The first couple of lines.,.
          Sometimes:
            Cache Database Integrity Check on 11/04/2018 at 19:00:01
            System: HOSTNAME  Configuration: INSTANCENAME
            Cache for OSTYPE (OS Information) CACHE_VERSION (Build NUMBER) BUILD_DATE
          Othertimes only two lines:
            Cache Database Integrity Check on 11/20/2018 at 19:43:01
            System: SYS  Configuration: INSTANCENAME
          At least there's always a whitespace, so just ignore the third line if it's empty.
        """
        line = {}
        line[1] = fp.readline().rstrip().split(' ')
        line[2] = fp.readline().rstrip().split(' ')
        line[3] = fp.readline().rstrip().split(' ') 
        # First 3 lines, we're lazy, there's probably a better way, but this works.
        # Makes sure that those first three lines are 'valid'.
        if ' '.join(line[1][0:5]) != "Cache Database Integrity Check on":
            sys.exit("Unusual line#1 at the top of integfile: {}".format(integ_file))
        if line[2][0] != "System:":
            sys.exit("Unusual line#2 at the top of integfile: {}".format(integ_file))
        if line[3][0] != "Cache":
            if not line[3][0]:
                # Sometimes line[3] doesnt exist at all, but it either not-exists or is "Cache"
                pass
            else:
                sys.exit("Unusual line#3 at the top of integfile: " + integ_file)
        
        integfile_data = {}
        integfile_data["databases"] = {}
        integfile_data["profile"] = {}

        integfile_profile = {}    
        integfile_profile["Start_Date"] = line[1][5]
        integfile_profile["Start_Time"] = line[1][7]
        integfile_profile["End_Date"] = line[1][5]
        integfile_profile["End_Time"] = line[1][7]
        integfile_profile["System_Name"] = line[2][1] 
        # There's a weird double space on the next line, we need to account for it
        #   eg: System: HOSTNAME  Configuration: CACHE
        integfile_profile["System_Instance"] = line[2][4] 
        if line[3][0]: 
            # Remember, line 3 either exists with a version or is blank.
            integfile_profile["System_Version"] = line[3][9] 
        integfile_profile["Elapsed_Seconds"] = 0
        integfile_data["profile"].update(integfile_profile)

        for line in fp:
            line = line.rstrip() 
            if not line:
                """
                skip blank lines
                """
                continue
            
            elif line[0] == 'N':
                """ 
                N: End of the file, and there were no errors.
                  NB: The 'No Errors were found in this directory.' lines are handled in the 
                    end-of-directory sections below.
                     eg: "^No Errors were found.$"
                """
                continue
            
            
            elif line[0] == '*':
                """
                *: End of the file, and there were errors
                  We dont have to deal with errors at the end of the file, they're just 
                   repeats of errors we found during the run.
                  Just quit, no need to do any work here
                """
                break
            
            
            elif line[0] == '-':
                """
                -: Start or End of a database check
                  eg: 
                    Start: "^---Directory /usr/cachesys/mgr/user/---$"
                    End:   "^---Total for directory /usr/cachesys/mgr/user/---$"
                """
                if "Directory" in line.split()[0].strip('-'):
                    database = line.split()[1].strip('-')
                    integfile_data["databases"][database] = {}
                    integfile_data["databases"][database]["globals"] = {}
                    integfile_data["databases"][database]["totals"] = {}
                elif "Total" in line.split()[0].strip('-'):
                    endofdatabase = deal_with_endofdatabase(fp)
                    integfile_data["databases"][database]["totals"].update(endofdatabase)
                    # At the end of each database, update the end_date/time fields
                    #  This helps because I'm lazy and dont want to do it at the end of the file.
                    integfile_data["profile"]["End_Date"] = endofdatabase["End_Date"]
                    integfile_data["profile"]["End_Time"] = endofdatabase["End_Time"]
                    integfile_data["profile"]["Elapsed_Seconds"] += endofdatabase["Elapsed_Seconds"]
                    # database_errors() returns a dict in the form {'errors': {'global': 'error text'}}.
                    database_errors = deal_with_database_errors(fp) 
                    if database_errors:
                        integfile_data["databases"][database].update(database_errors)
                        if not ("errors" in integfile_data):
                            integfile_data["errors"] = {}
                        if not (database in integfile_data["errors"]):
                            integfile_data["errors"][database] = {}
                        integfile_data["errors"][database].update(database_errors["errors"])
            
            #  It's a global entry
            elif 'Global' in line.split(' ')[0]:
                global_name = line.strip().split(' ')[1]
                global_values = deal_with_global(fp, global_name)
                integfile_data["databases"][database]["globals"][global_name] = {}
                integfile_data["databases"][database]["globals"][global_name].update(global_values)
            
            else:
                sys.exit("Unexpected value in deal_with_integfile:" + line)
   
    return integfile_data
    
    
def deal_with_global(fp, global_name):
    """ Collects values of a Global from the integ file, returns JSON. """
    global_values = {}
    global_values["Data"] = {}
    global_values["Time"] = {}
    for line in fp:
        line=line.strip()
        if not line:
            continue
        elif line[0] == "*":
            """
            Starting with a * means this line is an error.
             example error:            
                **********Global CacheTempClassDesc is Not OK**********
                 The pointer block contains the wrong global
                 ^G^G
            
             Read until the blank line, that's all of the error.  
             We can scrap the first line with the asterix, we already know the global.
            """
            global_values["errors"]={}
            error_msg = ""
            for line in fp:
                if line.strip() == "":
                    global_values["errors"] = error_msg
                    break ##
                else:
                    error_msg = error_msg + " " + line
            
        elif "Elapsed Time" in str(line.strip()):
            """ 
            If the string starts with 'Elapsed Time ='
            that's the end of the global entry, eg:
                Elapsed Time = 0.0 seconds, Completed 11/04/2018 19:00:01
            OLD VERSIONS OF CACHE:
                Elapsed Time = 0.0 seconds 19:43:01.
            """
            line=line.split()
            if line[5] == "Completed":
                global_values["Time"]["Elapsed_Seconds"] = float(line[3])
                global_values["Time"]["End_Date"] = line[6]
                global_values["Time"]["End_Time"] = line[7]
            else:
                global_values["Time"]["Elapsed_Seconds"] = float(line[3])
                global_values["Time"]["End_Date"] = "UNKDATE"
                global_values["Time"]["End_Time"] = line[5]
            break ##
            
        else:
            """
             Not elapsed time, one of the other fields
              eg:
                Global: EXAMPLE
                 Top/Bottom Pnt Level: # of blocks=1      8kb (0% full)
                 Data Level:           # of blocks=1      8kb (0% full)
                 Big Strings:            # of blocks=10     80kb (80% full) # = 2
                 Total:                # of blocks=12     96kb (67% full)
                 Elapsed Time = 0.0 seconds, Completed 11/04/2018 19:00:01
            """
            line = line.split(':')
            field = line[0].strip()
            field = re.sub('[^0-9a-zA-Z]+', '_', field)
            value_tmp = line[1].strip().split()
            # Value contains: '# of blocks=1      8kb (0% full)'
            global_values["Data"][field] = {}
            global_values["Data"][field]["Blocks"] = int(re.sub('blocks=|,', '', value_tmp[2]))
            #global_values[field]["Size_KB"] = re.sub('[kb,]', '', value_tmp[3]) 
            
            size = value_tmp[3]
            size_symbol = re.sub('[0-9,]', '', size) # ie: 2,349kb becomes kb
            multiplier = {
              'kb': 1,
              'MB': int(1024),
              'GB': int(1024**2),
              'TB': int(1024**3),
              'PB': int(1024**4),
              'EB': int(1024**5)
            }[size_symbol] # If you have more than exabytes in one database seek profressional help because dang
            size_in_kb = int(re.sub('[^0-9]', '', size)) * multiplier
            global_values["Data"][field]["Size_KB"] = int(size_in_kb)
            # Turn '(80%' into 80 and make it an int.  
            #  The int-casting is because the JSON output will think it's a str otherwise (?)
            percent_full = int(re.sub('[\%\(]', '', value_tmp[4]))
            global_values["Data"][field]["Percent_Full"] = percent_full
            if field == "Big_Strings":
                # If field is big strings, we need that number on the end too, that is the 
                #   total number of big strings in use.
                big_stings_count = int(re.sub('blocks=|,', '', value_tmp[8]))
                global_values["Data"][field]["Big_Stings_Count"] = big_stings_count
    
    return global_values
    
def deal_with_endofdatabase(fp):
    """ Process end of Database section, returns it JSON formatted """
    """
     End of the database
        AIX:
        ---Total for directory /db/database/---
               368 Pointer Level blocks        2944kb (11% full)
            26,364 Data Level blocks            205MB (79% full)
               475 Big String blocks           3800kb (63% full) # = 393
            27,222 Total blocks                 212MB (77% full)
             2,090 Free blocks                   16MB
    
        Elapsed time = 1.4 seconds 11/04/2018 19:00:04
    
        No Errors were found in this directory.
        
        VMS:
        ---Total for directory _DIR:[DATABASE]---
            23,753 Pointer Level blocks         185MB (67% full)
         8,805,663 Data Level blocks          68794MB (72% full)
             1,556 Big String blocks             12MB (67% full) # = 1,296
         8,831,852 Total blocks               68998MB (72% full)
            54,728 Free blocks                  427MB

        Elapsed time = 7765.6 seconds 21:52:27

        No Errors were found in this directory.
    """
    return_dict = {}
    for line in fp:
        line = line.strip()
        if not line:
            continue
        else:
            if line.split(' ')[0] == "Elapsed":
                # End of the data for the database, return the value after getting the 
                #  elapsed times
                # Remember that there are 7 entries, but it's indexed from 0
                if len(line.split(' ')) == 7: 
                    Elapsed_Seconds = float(line.split(' ')[3])
                    End_Date = line.split(' ')[5]
                    End_Time = line.split(' ')[6]
                elif len(line.split(' ')) == 6:
                    # old/vms Cache version.  No date!
                    Elapsed_Seconds = float(line.split(' ')[3])
                    End_Date = "UNKDATE"
                    End_Time = line.split(' ')[5]
                else:
                    sys.exit("Issue with the length of line in deal_with_endofdatabase. Line:" + line)
                
                return_dict.update({
                    "Elapsed_Seconds": Elapsed_Seconds, 
                    "End_Date": End_Date, 
                    "End_Time": End_Time
                    })
                break
                
            else:
                # replace multiple spaces with a :
                #  line becomes: 368 Pointer Level blocks:2944kb (11% full)
                line = re.sub('  +', ':', line) 
                value = line.split(' ')[0]
                value = re.sub('[,]', '', value)
                # Try to cast value as an int, then a float.  nb: I dont think this works.
                try:
                    value = int(value)
                except:
                    try: 
                        value = float(value)
                    except:
                        value = str(value)
                # turn "368 Pointer Level blocks:2944kb (11% full)" into 
                #   "368 Pointer Level blocks":
                field = line.split(':')[0] 
                # Now turn "368 Pointer Level blocks" into "Pointer_Level_blocks":
                field = field.split(' ',1)[1] 
                field = re.sub('[ ]', '_', str(field))
                # Now add '"Pointer_Level_blocks": 368' to our return_dict
                return_dict.update( { field: value } )
    
    return return_dict

                
def deal_with_database_errors(fp):
    """ Checks to see if there are database errors, returns JSON of said errors. """ 
    """
    Deal with all of the errors we just detected.
    No Errors Example:
        No Errors were found in this directory.

    Errors Example:
        ***** The following errors were detected *****
         **********Global CacheTempClassDesc is Not OK**********
         The pointer block contains the wrong global
    """
    return_dict = {}
    for line in fp:
        line = line.strip()
        if not line:
            continue
        else:
            first_word=line.split(' ')[0]
            if first_word == 'No':
                break
            elif first_word == '*****':
                pass # Skip the first line we just got, it's just telling us there are errors
            elif first_word == "**********Global":
                global_with_errors = line.split(' ')[1]
                errormessage = "" # reset error message
                for line in fp:
                    line=line.strip()
                    if line == "":
                        # End of error message
                        return_dict["errors"] = {}
                        return_dict["errors"].update({global_with_errors :  errormessage})
                        break
                    else:
                        errormessage = errormessage + " " + line
    
    return return_dict
                
def deal_with_endoffile_errors(fp):
    """ Does nothing, Returns nothing. 
    
    The errors here are just repeats of errors encountered during the main file.
    Those errors are already handled.  So we can just quit.
    """
   
    """
    *****ERRORS WERE FOUND *****
    
    ***** The following errors were detected *****
    
    ************************************************
    *** Errors in directory: /db/databases/appqcpn/ ***
    ************************************************
    
     **********Global CacheTempClassDesc is Not OK**********
     The pointer block contains the wrong global
    """
 
    return


def parse_args(args):
    """ Deal with the args."""

    parser = argparse.ArgumentParser(
                        description='Convert Intersystems Cache Integrity Files to JSON')
    parser.add_argument('-s', '--singlefile', 
                        help='Store everything in a single JSON file (-o is mandatory).', 
                        action='store_true')
    parser.add_argument('-d', '--deidentify', 
                        help='Deidentify databases/globals/etcetc',
                        choices=['hash', 'generic'])
    parser.add_argument('-b', '--beautify', 
                        help='Beautify JSON output (significantly increases size of output file)', 
                        action='store_true', 
                        default=False)
    parser.add_argument('-o', '--outdir', 
                        help='Location to put the JSON files (default is location of integ file)',
                        metavar='outdir',
                        type=Path)
    parser.add_argument('files', 
                        help='List of Integrity files',
                        nargs="+")
    args = parser.parse_args()
    
    """Sanity check the arguments"""
    if args.singlefile:
        if not args.outdir:
            sys.exit("If singlefile flag is used, outdir must be set.  Exiting...")
    if args.outdir:
        if not args.outdir.is_dir():
            sys.exit("File path {} does not exist.  Exiting...".format(args.outdir))
        # Make sure we can write to the directory
        try:
            testfile = tempfile.TemporaryFile(dir = args.outdir)
            testfile.close()
        except OSError as e:
            if e.errno == errno.EACCES:
                sys.exit("Files path {} is not writeable.  Exiting...".format(args.outdir))
            else:
                sys.exit("File path {} is not writeable, for unknown reasons. Sorry.  Exiting...".format(args.outdir))
    return args
       
if __name__ == '__main__':  
   args = parse_args([*sys.argv[1:]])
   main(args)

   
