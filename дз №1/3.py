def process_line(line):
    numbers = []
    elements = line.split()
    for elem in elements:
        numbers.append(None if elem == "N/A" else int(elem))


    for i in range(len(numbers)):
        if numbers[i] is None:
            left = numbers[i - 1] if i > 0 else None
            right = numbers[i + 1] if i < len(numbers) - 1 else None
            if left is not None and right is not None:
                numbers[i] = (left + right) // 2
            elif left is not None:
                numbers[i] = left
            elif right is not None:
                numbers[i] = right


    filtered_numbers = [num for num in numbers if num % 7 == 0]
    return filtered_numbers


input_path = 'C:/Users/kit/Desktop/third_task.txt'
output_path = 'C:/Users/kit/Desktop/3.txt'

with open(input_path, 'r') as file:
    lines = file.readlines()


all_filtered = []
for line in lines:
    filtered = process_line(line)
    all_filtered.extend(filtered)

# Сохраняем результат в файл
with open(output_path, 'w') as output_file:
    for num in all_filtered:
        output_file.write(f"{num}\n")

output_path