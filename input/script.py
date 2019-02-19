import random

# random.seed(0)
lines_to_read = set()

end = 100000

def generate_sample_data(size, fro, to):
  while len(lines_to_read) != end:
    x = random.randint(1, size) # ques: 17203824 tags: 38648235 final: 4510225
    lines_to_read.add(x)

  print("starting to write to {}".format(to))
  with open(to, "w") as fp:
    with open(fro, "r") as f:
      for i, line in enumerate(f):
        if i in lines_to_read:
          fp.write(line)

to = "question_tags_sample.txt"
fro = "question_tags.txt"
generate_sample_data(38648235, fro, to)

to = "questions_sample.txt"
fro = "questions.txt"
generate_sample_data(17203824, fro, to)

