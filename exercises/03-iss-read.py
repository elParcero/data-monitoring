'''
    Exercise 3:

        Parse the data in data/xas_data/xas_test_328.txt
        This data consists of a header and the data itself

        1. Read the header
        Save all the lines that begin with "#" to a dictionary:
            ex:  this line
                # Year: 2018
            should go to md['Year'] = '2018'
        Save the header data to a dictionary

        2. The last commented line contains the names of the columns for the
        data
        save these in a list

        3. The rest of the data comes in 4 columns
            save these each to a list

        4. Finally, create a pandas dataframe of this data

        5. Plot the data
            can you plot:
                np.log(df.it/df.i0)

            what does it look like?
'''
