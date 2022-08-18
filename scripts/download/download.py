import argparse
import sys
import os.path
import csv
import hashlib
import tarfile
from pathlib import Path
import gdown
import wget
import sys, os
from multiprocessing import Pool
from functools import partial

base_path = Path(__file__).parent
ID_FILE_PATH = (base_path / "../../data/download/").resolve()

download_from_names = {'gdrive': 'GDrive', 'mtg': 'MTG'}


def compute_sha256(filename):
    with open(filename, 'rb') as f:
        contents = f.read()
        checksum = hashlib.sha256(contents).hexdigest()
        return checksum
    return None

    raise Exception('Error computing a checksum for %s' % filename)


def _download(filename,dataset, data_type, download_from, output_dir,
              quiet=False, alternative_basepath = ('/media/giac/Volume','/home/giac/tmp')):
    try:
        output = os.path.join(output_dir, filename)

        if os.path.exists(output):
            print('Skipping %s (file already exists)' % output)
            return output
        elif os.path.exists(output.replace(*alternative_basepath)):
            output = output.replace(*alternative_basepath)
            print('Skipping %s (file already exists)' % output)
            return output

        if download_from == 'gdrive':
            url = 'https://drive.google.com/uc?id=%s' % filename#ids[filename]
            gdown.download(url, output, quiet=quiet)

        elif download_from == 'mtg':
            url = 'https://essentia.upf.edu/documentation/datasets/mtg-jamendo/' \
                  '%s/%s/%s' % (dataset, data_type, filename)
            if quiet:
                wget.download (url, out=output, bar=False)
            else:
                wget.download (url, out=output)
        return output
    except:
        return False


def download(dataset, data_type, download_from, output_dir,
             parallel=False, quiet=False, alternative_basepath = ('/media/giac/Volume','/home/giac/tmp'), validate=False):

    os.makedirs(output_dir,exist_ok=True)
    if alternative_basepath[1] in output_dir:
        alternative_basepath = (alternative_basepath[1],alternative_basepath[0])
    print('Downloading %s from %s' % (dataset, download_from_names[download_from]))
    file_gids = os.path.join(ID_FILE_PATH, dataset + '_' + data_type + '_gids.txt')
    file_sha256_tars = os.path.join(ID_FILE_PATH, dataset + '_' + data_type + '_sha256_tars.txt')

    # Read checksum values for tars and files inside.
    with open(file_sha256_tars) as f:
        sha256_tars = dict([(row[1], row[0]) for row in csv.reader(f, delimiter=' ')])

    # Read filenames and google IDs to download.
    ids = {}
    with open(file_gids, 'r') as f:
        for line in f:
            id, filename = line.split(('   '))[:2]
            ids[filename] = id

    if parallel:
        print("There are {} CPUs on this machine ".format(os.cpu_count()))
        pool = Pool(os.cpu_count())
        download_func = partial(
            _download,
            dataset=dataset,
            data_type=data_type,
            download_from='mtg',
            output_dir=output_dir,
            quiet=True,
            alternative_basepath=alternative_basepath
        )
        results = pool.map(download_func, ids.keys())
        pool.close()
        pool.join()
    else:
        results = []

    to_do_again = []

    for filename in ids:
        if not parallel:
            results.append(
                _download(
                    filename,
                    dataset,
                    data_type,
                    download_from,
                    output_dir,
                    quiet,
                    alternative_basepath
                )
            )

        if not results[-1]:
            continue

    for output, filename in zip(results, ids):
        if not output:
            to_do_again.append(filename)
            continue

        if validate:
            # Validate the checksum.
            if compute_sha256(output) != sha256_tars[filename]:
                sys.stderr.write('%s does not match the checksum, -debug- not removing the file' % output)
                to_do_again.append(filename)
                os.remove(output)
            else:
                print('%s checksum OK' % filename)
    if to_do_again:
        print('Missing files:', ' '.join(to_do_again))
        print('Re-run the script again')
        return
    print('Download complete')
    return results

def unpack_tar(filename, remove_tar, sha256_tracks=None):
    output_dir ='/'.join(filename.split('/')[:-1])
    print('Unpacking', filename)
    tar = tarfile.open(filename)
    tracks = tar.getnames()[1:]  # The first element is folder name.
    tar.extractall(path=output_dir)
    tar.close()
    # Validate checksums for all unpacked files
    valid_tracks=[]
    if sha256_tracks:
        for track in tracks:
            trackname = os.path.join(output_dir, track)
            if compute_sha256(trackname) != sha256_tracks[track]:
                sys.stderr.write('%s does not match the checksum' % trackname)
                raise Exception('Corrupt file in the dataset: %s' % trackname)
            else:
                valid_tracks.append(trackname)
        print('%s track checksums OK' % filename)
    if remove_tar:
        os.remove(filename)
    return valid_tracks

def unpack_tars(filenames,dataset, data_type,remove_tars):

    print('Unpacking tar archives')
    file_sha256_tracks = os.path.join(ID_FILE_PATH, dataset + '_' + data_type + '_sha256_tracks.txt')
    with open(file_sha256_tracks) as f:
        sha256_tracks = dict([(row[1], row[0]) for row in csv.reader(f, delimiter=' ')])

    tracks_checked = []
    for filename in filenames:
        tracks_checked += unpack_tar(filename,remove_tar=remove_tars, sha256_tracks=sha256_tracks)

    print('Unpacking complete')



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download the MTG-Jamendo dataset',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--dataset', default='raw_30s', choices=['raw_30s', 'autotagging_moodtheme'],
                        help='dataset to download')
    parser.add_argument('--type', default='audio', choices=['audio', 'melspecs', 'acousticbrainz'],
                        help='type of data to download (audio, mel-spectrograms, AcousticBrainz features)')
    parser.add_argument('--from', default='gdrive', choices=['gdrive', 'mtg'],
                        dest='download_from',
                        help='download from Google Drive (fast but bumpy) or MTG (server in Spain, slow but stable)')
    parser.add_argument('outputdir', help='directory to store the dataset')
    parser.add_argument('--unpack', action='store_true', help='unpack tar archives')
    parser.add_argument('--remove', action='store_true', help='remove tar archives while unpacking one by one (use to save disk space)')

    args = parser.parse_args()
    ids = download(args.dataset, args.type, args.download_from, args.outputdir)
    if args.unpack:
        unpack_tars(ids,args.dataset,args.output_dir,args.remove)
