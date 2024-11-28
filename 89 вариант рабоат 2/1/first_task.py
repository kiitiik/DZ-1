import numpy as np

# Загрузим матрицу из файла
matrix = np.load('C:/Users/kit/Desktop/first_task.npy')

# Подсчитаем сумму и среднее арифметическое всех элементов
matrix_sum = np.sum(matrix)
matrix_mean = np.mean(matrix)

# Главная диагональ (sumMD, avrMD)
main_diag_sum = np.sum(np.diag(matrix))
main_diag_mean = np.mean(np.diag(matrix))

# Побочная диагональ (sumSD, avrSD)
side_diag_sum = np.sum(np.diag(np.fliplr(matrix)))
side_diag_mean = np.mean(np.diag(np.fliplr(matrix)))

# Найдем максимальное и минимальное значение
matrix_max = np.max(matrix)
matrix_min = np.min(matrix)

# Создаем результат в формате JSON
result = {
    "sum": matrix_sum,
    "avr": matrix_mean,
    "sumMD": main_diag_sum,
    "avrMD": main_diag_mean,
    "sumSD": side_diag_sum,
    "avrSD": side_diag_mean,
    "max": matrix_max,
    "min": matrix_min
}

# Нормализуем матрицу: приведение значений к диапазону [0, 1]
normalized_matrix = (matrix - matrix_min) / (matrix_max - matrix_min)

# Сохраним нормализованную матрицу в формате npy
normalized_matrix_path = 'C:/Users/kit/Desktop/first_task-result.npy'
np.save(normalized_matrix_path, normalized_matrix)

result, normalized_matrix_path