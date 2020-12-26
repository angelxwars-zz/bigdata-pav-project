import os


def get_random_tree_graph():
    a_file = open("../../ficheros/2b-random_forest.txt", "r")
    lines = a_file.readlines()
    a_file.close()

    if 'TreeEnsembleModel regressor with' in lines[0]:
        del lines[0]

    new_file = open("./random_forest.txt", "w+")
    for line in lines:
        new_file.write(line)
    new_file.close()

    os.system('eurekatrees --trees "./random_forest.txt" --output_path ../Graficas/random_forest')
    os.remove("./random_forest.txt")

if __name__ == '__main__':
    get_random_tree_graph()

