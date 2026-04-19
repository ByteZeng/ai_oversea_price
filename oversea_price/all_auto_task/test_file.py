import time


def t1():
    a = 2 / 0
    print(a)
    print('one')


def t2():
    a = 2 + 'i'
    print(a)
    print('two')


def t3():
    print('three')


def t4():
    print('four')


def t5():
    print('five')


def t6():
    print('six')


def t7():
    print('seven')


def t8():
    time.sleep(30)
    print('eight')


def t9():
    print('nine')


if __name__ == '__main__':
    t1()
