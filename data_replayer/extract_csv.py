import zipfile, os
src = os.path.abspath('../data/raw/UserBehavior.csv.zip')
dst = os.path.abspath('../data/raw/')
print(f"Extracting {src} -> {dst} ...")
with zipfile.ZipFile(src) as z:
    z.extractall(dst)
print("Done.")
