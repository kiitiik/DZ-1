import numpy as np

matrix_2 = np.load('C:/Users/kit/Desktop/second_task.npy')
threshold = 589
indices = np.argwhere(matrix_2 > threshold)
rows = indices[:, 0]
cols = indices[:, 1]
values = matrix_2[indices[:, 0], indices[:, 1]]

npz_path = 'C:/Users/kit/Desktop/matrix_filtered.npz'
npz_compressed_path = 'C:/Users/kit/Desktop/matrix_filtered_compressed.npz'

np.savez(npz_path, rows=rows, cols=cols, values=values)
np.savez_compressed(npz_compressed_path, rows=rows, cols=cols, values=values)

import os
size_npz = os.path.getsize(npz_path)
size_npz_compressed = os.path.getsize(npz_compressed_path)
(size_npz, size_npz_compressed)