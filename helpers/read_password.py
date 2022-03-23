def read_password():
    f = open("helpers/pass.txt", "r")
    lines = f.readlines()
    username = lines[0]
    password = lines[1]
    f.close()

    return username, password
