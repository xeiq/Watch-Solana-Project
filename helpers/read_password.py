def read_password():
    f = open("helpers/pass.txt", "r")
    lines = f.readlines()
    username = lines[0].strip()
    password = lines[1].strip()
    f.close()

    return username, password
