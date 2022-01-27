import os
import shutil
import hashlib
import filecmp
from hashlib import md5
from pathlib import Path
import zipfile
from zipfile import ZipFile
from multiprocessing import Process
import datetime

#Timer to list how long process takes
start_time = datetime.datetime.now()
x = 0
for i in range(1000):
   x += i

#Define filepaths where path= folder containing data to be archived, zipped_path for zip folders to be moved to if they match with original, folder for md5 files
path='/home/david/Documents/archiver_zip/input/'
zipped_path='/home/david/Documents/archiver_zip/zipped/'
md5_path='/home/david/Documents/archiver_zip/md5/'

Path(zipped_path).mkdir(parents=True, exist_ok=True)
Path(md5_path).mkdir(parents=True, exist_ok=True)

#Make zips of all folders in path variable
def make_zip_archive(dir_name):
    shutil.make_archive(path+dir_name, 'zip', path+dir_name)


def original_folder_md5_check(item):
    checksums=[]
    if os.path.isdir(path+item):
        original_folder = path+item
        orig_folder=item
        for root, dirs, files in os.walk(original_folder):
            for x in files:
                FileNames = (os.path.join(root, x))
                file_array=FileNames.split('\n')
                #print(file_array)
                for file in file_array:
                    with open(file, 'rb') as check_file:
                        checksums.append([file, hashlib.md5(check_file.read()).hexdigest()])

                #print(checksums)
                with open(orig_folder + "_md5.txt", "w") as text_file:
                    for line in checksums:
                        text_file.write(" ".join(line) + "\n")

def zipfolder_md5_check(zipitem):
    checksums=[]
    #for zipitem in os.listdir(path):
    if zipfile.is_zipfile(path+zipitem):
        with zipfile.ZipFile(path+zipitem, 'r') as zip_ref:
            zipped_file=zipitem
            zipped_folder_name=zipitem.replace(".zip", "")
            zipfile.ZipFile(path+zipped_file).extractall(path+zipped_folder_name+"_temp")
            for root, dirs, files in os.walk(path+zipped_folder_name+"_temp"):
                for y in files:
                    FileNames = (os.path.join(root, y))
                    file_array=FileNames.split('\n')
                    for file in file_array:
                        with open(file, 'rb') as check_file:
                            checksums.append([file, hashlib.md5(check_file.read()).hexdigest()])
                    with open(zipped_folder_name + "_zip_md5.txt", "w") as text_file:
                        for line in checksums:
                            edited_line = [item.replace("_temp", "") for item in line]
                            text_file.write(" ".join(edited_line) + "\n")

#compare the original folder contents with the zipfolder to ensure zip process completed successfully
#Zipfolders and md5 files are only moved if the original folder contents and zipfolder contents match.
def move_matching_zipfolders():
    for item in os.listdir("./"):
        if item.endswith("_zip_md5.txt"):
            zipped_folder_md5sum_file = item
            x=item.split('_zip_md5.txt')
            zipped_folder_name=x[0]+'.zip'
            original_folder_name=x[0]
            original_folder_md5sum_file = x[0]+'_md5.txt'
            filecompare_result = filecmp.cmp(original_folder_md5sum_file, zipped_folder_md5sum_file, shallow=False)

            if filecompare_result == True:
                print("successful md5 match for " + original_folder_name + " contents and " + zipped_folder_name + " contents")
                original_filepath = path+zipped_folder_name
                zipfolder_filepath = zipped_path+zipped_folder_name
                shutil.move(original_filepath, zipfolder_filepath)
                shutil.move(original_folder_md5sum_file, md5_path+original_folder_md5sum_file)
                shutil.move(zipped_folder_md5sum_file, md5_path+zipped_folder_md5sum_file)

            if filecompare_result == False:
                print("problem with md5 match for " + original_folder_name + " and " + zipped_folder_name)

#Tidy up temp folder
def remove_temporary_folders():
    for item in os.listdir(path):
        if item.endswith("_temp"):
            shutil.rmtree(path+item)

def processing_time():
    end_time = datetime.datetime.now()
    time_diff = (end_time - start_time)
    execution_time = time_diff.total_seconds()
    print(execution_time, " seconds to process")

if __name__ == "__main__":
    #process_list=[]
    #for dir_name in os.listdir(path):
    #    p=Process(target=make_zip_archive, args=(dir_name,))
    #    p.start()
    #    process_list.append(p)
    #for process in process_list:
    #    process.join()

    #procs=[]
    #for item in os.listdir(path):
    #    proc=Process(target=original_folder_md5_check, args=(item,))
    #    proc.start()
    #    procs.append(proc)
    #for proc in procs:
    #    proc.join()

    processes=[]
    for zipitem in os.listdir(path):
        p=Process(target=zipfolder_md5_check, args=(zipitem,))
        p.start()
        processes.append(p)
    for process in processes:
        process.join()
    #zipfolder_md5_check()
    #move_matching_zipfolders()
    #remove_temporary_folders()
    processing_time()
