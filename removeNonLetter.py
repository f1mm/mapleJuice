import sys

def remove_non_letter(filename):
    f=open(filename,'r')
    data=f.read()
    f.close()
    letters = list([val.lower() if val.isalpha() else ' ' for val in data ]) 
    filtered_text = "".join(letters) 
    new_file = open("filtered_" + filename, "w")
    n = new_file.write(filtered_text)
    new_file.close()

if __name__ == '__main__':
    remove_non_letter(*sys.argv[1:])