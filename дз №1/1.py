from collections import Counter
import re


file_path = 'C:/Users/kit/Desktop/first_task.txt'


with open(file_path, 'r') as file:
    text = file.read()


words = re.findall(r'\b\w+\b', text.lower())


word_freq = Counter(words)


sorted_word_freq = sorted(word_freq.items(), key=lambda x: (-x[1], x[0]))


sentences = re.split(r'[.!?]', text)
sentence_lengths = [len(re.findall(r'\b\w+\b', sentence)) for sentence in sentences if sentence.strip()]
average_words_per_sentence = sum(sentence_lengths) / len(sentence_lengths) if sentence_lengths else 0

sorted_word_freq[:10], average_words_per_sentence

output_path = 'C:/Users/kit/Desktop/result_1.txt'


with open(output_path, 'w') as output_file:
    for word, freq in sorted_word_freq:
        output_file.write(f"{word}:{freq}\n")
    output_file.write("\n")
    output_file.write(f"среднее количество слов в предложении: {average_words_per_sentence:.2f}\n")

output_path