#### This is a simple Python program that checks if a given integer is even or odd. It also includes error handling to ensure that the user inputs a valid integer.

print('Hello World')


try:
    n = int(input('Enter integer to check if it is even: '))
except ValueError:
    print(f'Invalid input. Please enter an integer.')
    exit()

else:
    if n%2 == 0:
        print('Even')
    else:    
        print('Odd')