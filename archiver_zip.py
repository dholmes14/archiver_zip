import os
import shutil
import hashlib
import filecmp
from hashlib import md5
from pathlib import Path
import zipfile
from zipfile import ZipFile
import glob
from multiprocessing import Process
import datetime


start_time = datetime.datetime.now()
x = 0
for i in range(1000):
   x += i

path='/mnt/c/Users/david/Documents/data_archiving/data_for_aws_archive/'
zipped_path='/mnt/c/Users/david/Documents/data_archiving/zipped_data_for_aws_archive/'
md5_path='/mnt/c/Users/david/Documents/data_archiving/md5_files/'

md5_hash = hashlib.md5()

Path(zipped_path).mkdir(parents=True, exist_ok=True)
Path(md5_path).mkdir(parents=True, exist_ok=True)

directory_contents = os.listdir(path)

for dir_name in directory_contents:
    shutil.make_archive(path+dir_name, 'zip', path+dir_name)

for item in os.listdir(path):
    #print(item)
    if os.path.isdir(path+item):
        original_folder = path+item
        orig_folder=item
        for root, dirs, files in os.walk(original_folder):
            for x in dirs:
                dirlist= (os.path.join(root, x)+'/'+'\n')
                #print(dirlist)
                with open(orig_folder + "_contents.txt", "a") as text_file:
                    for item in dirlist:
                        text_file.write(item)
            for y in files:
                FileNames = (os.path.join(root, y)+'\n')
                #print(FileNames)
                with open(orig_folder + "_contents.txt", "a") as text_file:
                    for item in FileNames:
                        text_file.write(item)

        md5_hash = hashlib.md5()
        original_contents_file = open(orig_folder + "_contents.txt", "rb")
        content = original_contents_file.read()
        md5_hash.update(content)
        original_folder_md5sum = md5_hash.hexdigest()
        print(original_folder_md5sum + " : " + orig_folder )
        with open(orig_folder + "_md5.txt", "w") as md5sum_file:
            md5sum_file.write(original_folder_md5sum)
            original_folder_md5sum_file=orig_folder + "_md5.txt"

    if zipfile.is_zipfile(path+item):
        zip_fullname=item
        zipped_file=item.replace(".zip", "")
        with ZipFile(path+item, 'r') as zipObj:
            listOfiles = zipObj.infolist()
            docx_name_list = zipObj.infolist()
            paths_to_images = [os.path.join(path, zipped_file, x.filename) for x in docx_name_list]
            with open(zipped_file+"_zip_contents.txt", "w") as text_file:
                for element in paths_to_images:
                    text_file.write(element + "\n")
                text_file.close()

        md5_hash = hashlib.md5()
        zipped_contents_file = open(zipped_file + "_zip_contents.txt", "rb")
        zipcontent = zipped_contents_file.read()
        md5_hash.update(zipcontent)
        zipped_folder_md5sum = md5_hash.hexdigest()
        print(zipped_folder_md5sum + " : " + zip_fullname )
        with open(zipped_file + "_zip_md5.txt", "w") as md5sum_file:
            md5sum_file.write(zipped_folder_md5sum)
            zipped_folder_md5sum_file=zipped_file + "_zip_md5.txt"

        filecompare_result = filecmp.cmp(original_folder_md5sum_file, zipped_folder_md5sum_file, shallow=False)
        if filecompare_result == True:
            print("successful md5 match for " + orig_folder + " contents and " + zip_fullname + " contents")
            if item.endswith(".zip"):
                original_filepath = path+item
                zipfolder_filepath = zipped_path+item
                shutil.move(original_filepath,zipfolder_filepath)
                shutil.move(original_folder_md5sum_file,md5_path+orig_folder+'_md5.txt')
                shutil.move(zipped_folder_md5sum_file,md5_path+zip_fullname+'_md5.txt')

        if filecompare_result == False:
            print("problem with md5 match for " + orig_folder + " and " + zip_fullname)

for item in os.listdir("./"):
    if item.endswith("contents.txt"):
        os.remove(item)

end_time = datetime.datetime.now()
time_diff = (end_time - start_time)
execution_time = time_diff.total_seconds()
print(execution_time, " seconds to process")
