"""
Read and pre-process the images and
save them to compressed .npz file.
"""

import os
from pathlib import Path
import struct
import cv2 as cv
import numpy as np
import pandas as pd
import pyodbc
from tqdm import tqdm
import tensorflow as tf

def handle_datetimeoffset(dto_value):
    """Datetime decoder for ODBC SQL."""
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 0, -6, 0)
    tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]
    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}".format(*tweaked)

def main():
    """Main module script."""

    # See TensorFlow version
    print(f"TensorFlow version: {tf.__version__}")
    # Check for TensorFlow GPU access
    print(f"TensorFlow has access to the following devices:\n{tf.config.list_physical_devices()}")

    # read the csv file with query results
    df = pd.read_csv(r'./data/query_results_01312024.csv', delimiter=',')
    print(df.info)

    img_dir = Path("./data/downloads")

    images, labels = [], []
    image_data_post = [str(s) for s in df['image_data_post'].tolist()]
    fail1_object_id = [int(i) for i in df['fail1_object_id'].fillna(0).tolist()]

    img_count = 0
    for im, fail in tqdm(zip(image_data_post, fail1_object_id), total=len(image_data_post)):
        if fail not in (9, 10, 14, 16):
            try:
                img_name = im.replace('ar_data/production/image_data/post/', '')
                img_path = os.path.join(img_dir, img_name)
                img = cv.imread(img_path, -1)

                if len(img.shape) == 3:
                    if img.shape[2] == 3:
                        img = cv.cvtColor(img, cv.COLOR_BGR2GRAY)

                img = cv.resize(img, (512, 512))

                #print(np.min(img), np.max(img), type(img), img.dtype, img.shape)

                lbl = 0 if fail == 0 else 1
                lbl = tf.keras.utils.to_categorical(lbl, 2)

                # append images and labels to lists
                images.append(img)
                labels.append(lbl)

                img_count += 1
            except Exception as e:
                print(f"Could not read image {img_path}")
                print(e)

    print(f"Saving {img_count} images to file.")

    # write data to csv
    np.savez_compressed(
        Path("./data/dataset_.npz"),
        images=images,
        labels=labels,
        classes=['pass', 'fail']
    )

if __name__ == "__main__":
    main()
