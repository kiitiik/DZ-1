import numpy as np

def average_negative(numbers):
    negative_numbers = [num for num in numbers if num < 0]
    return np.mean(negative_numbers) if negative_numbers else None

with open('C:/Users/kit/Desktop/second_task.txt', 'r') as file:
    lines = file.readlines()


row_results = []
for line in lines:
    numbers = list(map(int, line.split()))
    avg_neg = average_negative(numbers)
    if avg_neg is not None:
        row_results.append(avg_neg)


if row_results:
    min_value = min(row_results)
    max_value = max(row_results)
else:
    min_value = max_value = None

output_path = 'C:/Users/kit/Desktop/2.txt'
with open(output_path, 'w') as output_file:
    for result in row_results:
        output_file.write(f"{result:.2f}\n")
    output_file.write(f"\nMin: {min_value:.2f}\n")
    output_file.write(f"Max: {max_value:.2f}\n")

output_path