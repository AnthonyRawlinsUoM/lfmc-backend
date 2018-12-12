import subprocess
import os
from pathlib import Path


def expand_in_place(file_list, auto_remove=True):
    if auto_remove:
        keep = '-v'
    else:
        keep = '-k'

    for archive_file in file_list:

        target = archive_file.replace(".gz", "")
        if not Path(target).is_file():
            print("\n--> Expanding: %s" % archive_file)

            try:
                if str(archive_file).endswith('.gz'):
                    subprocess.run(['uncompress', keep, archive_file],
                                   shell=False, check=True)
                else:
                    print('Not a .gz file!')

            except FileNotFoundError as e:
                print("\n--> Expanding: %s, failed.\n%s" %
                      (archive_file, e))
                return False
            except OSError as e:
                print("\n--> Removing: %s, was not necessary.\n %s" %
                      (archive_file, e))
            return True
        else:
            if auto_remove:
                os.remove(archive_file)
            return True
