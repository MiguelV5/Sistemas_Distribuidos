line = "['author_1', 'author_n'],1960 \n"
authors, decade = eval(line)
expanded_authors = []
print(authors)
print(decade)
for author in authors:
    expanded_authors += author +','+ str(decade) + '\n'
print(str(expanded_authors))