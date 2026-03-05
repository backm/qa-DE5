def step1():
    data = [1,2,3]
    print(data)
    return data

def step2():
    result = [x * 2 for x in data]
    print(result)
    return result

def main():
    data = step1()
    result = step2(data)


    if __name__ =="__main__":
        main()