from query2 import QueryTwo
from query3 import QueryThree

print('One moment please.  Databases are being created.')

# create class objects
q1 = QueryTwo()
q2 = QueryThree()

print('Databases are ready')

# menu related
intro_phrase = 'Please type the number corresponding to the query you want to make.\nThe option to load files will be presented later.'
menu_options = '1. Query1\n2. Query2\nPress 0 to exit: \n'
print(intro_phrase)

while(True):
    print("_________________________________________")
    response = int(input(menu_options))
    print(response)
    if response == 0:
        exit()
    if response == 1:
        exit()
    if response == 2:
        q2.main_method()
    if response == 3:
        q3.main_method()
    if response == 4:
        exit()
