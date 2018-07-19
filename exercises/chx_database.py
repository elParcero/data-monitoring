import dataset
import sqlite3
import os


def readin_one_file(file_path):
    file_info = dict()
    index = 0
    with open(file_path) as file:
        for line in file:
            if index == 0:
                index += 1
                continue
            else:
                key_val = line.split(" ")
                file_info[key_val[0]] = float(key_val[1].replace('\n',''))
    return file_info


def storein_database(file_info, beamline_name ='chx'):
    db = sqlite3.connect('beamlines.db')
    db = dataset.connect('sqlite:///beamlines.db')
    table = db[beamline_name]

    for key in file_info:
        val = file_info[key]
        table.insert(dict(timestamp=key, fileusage=val))
        print(key)
    print('success in inserting values to table {}'.format(beamline_name))


def readin_mult_files(file_path, files):
    f_in = dict()
    file_info = dict()
    index = 0
    for file in files:
        with open(file_path + '/' + file) as file:
            for line in file:
                if index == 0:
                    name = line.split(',')[1].replace(':','_').replace('(fileusage)','').replace('\n','')
                    index += 1
                    continue
                else:
                    key_val = line.split(",")
                    f_in[key_val[0]] = float(key_val[1].replace('\n',''))
        file_info[name] = f_in
        name = ''
        f_in = {}
        index = 0
    return file_info


def store_multiples_filesin_db(files_in):
    db = sqlite3.connect('beamlines.db')
    db = dataset.connect('sqlite:///beamlines.db')
    for outer_key in files_in:
        table = db[outer_key]
        for inner_key in files_in[outer_key]:
            val = files_in[outer_key][inner_key]
            table.insert(dict(timestamp=inner_key, fileusage=val))
            print('{}|{}'.format(inner_key, val))
        print('success inserting values for table {}'.format(outer_key))


# chx file sizes
file_path = '/home/jdiaz/projects/data-monitoring/data/beamline_file_sizes/xf11id-ws2_file_sizes/data.dat'


# chx file sizes vs hour for plans + detectors
#file_path = '/home/jdiaz/projects/data-monitoring/exercises/plans_dets_fsize'
#files = [file for file in os.listdir(file_path) if file.endswith('.dat')]

file_info = readin_one_file(file_path)
#files_in = readin_mult_files(file_path, files)

storein_database(file_info)
#store_multiples_filesin_db(files_in)
