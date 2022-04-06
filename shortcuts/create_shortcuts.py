'''
Creates shortcuts for /vol/vvfelles/printobs/printobs -s <station> into .bash_aliases file on your local computer.

author: larsas@met.no, date: 06.04.22


'''

import getpass
import os
import os.path
import numpy as np
import argparse
import textwrap

parser=argparse.ArgumentParser(epilog=textwrap.dedent('''If you run this script (< python create_shortcuts.py > in local terminal) it will add shortcuts to run < printobs > to your local .bash_aliases file. I.e instead of writing for example: \n< /vol/vvfelles/printobs/printobs -s valhall >\n you can simply write < val > in your local terminal.
    
    
    
    \nIf you already have a .bash_aliases file it will add information to this file, and it will not overwrite anything. If you keep running this script it will add the shortcut information many times, so you end up with the same aliases many times. After running this script: open a new terminal and write < stations > in your local terminal to see your station abbrevation options.\n'''))

args=parser.parse_args()

#Get username
user= getpass.getuser()

#Name and path of file
ba = '/home/{}/.bash_aliases'.format(user)


#Make textfile to put setup into, unless it already exists
if os.path.exists(ba):

    print('File {} exist, appending information to your .bash_aliases file'.format(ba))
    append_write = 'a+' 
    print('\n')
    print('Appending shortcuts to {}...'.format(ba))
    print('\n')

else:
        append_write = 'w+' # make a new file if not
        print('\n')
        print('File does not exist making new .bash_aliases file here: {}'.format(ba))
        print('\n')



textfile = open(ba,append_write)

#Write information to textfile
textfile.write('''alias stations='printf "Station abbreviations\n\n1 asgardb = asgb \n2 aastahansteen = aasta \n3 brage = brg \n4 draugen = drg \n5 deepseaNK = dnk \n6 ekofiskL = eko \n7 gjoa = gjo \n8 goliat = gol \n9 grane = gra \n10 gudrun = gud \n11 gullfaksc = gull \n12 heidrun = heid \n13 heimdal = heim \n14 johansverdrup = josp \n15 kvitebjorn = kvit \n16 kristin = kris \n17 huldra = hul \n18 njorda = njo \n19 martinlingea = mla \n20 martinlingeb = mlb \n21 norne = nor \n22 ormenlange = orm \n23 oseberg = oseb \n24 osebergc = osebc \n25 osebergsyd = osebs \n26 petrojarl = pet \n27 scarabeo8 = sc8 \n28 snorrea = snoa \n29 snorreb = snob \n30 sleipnera = slea \n31 sleipnerb = sleb \n32 statfjorda = stata \n33 statfjordb = statb \n34 trolla = troa \n35 trollb = trob \n36 trollc = troc \n37 ula = ula \n38 valhall = val \n39 veslefrikk = vesl \n40 vis = visund \n41 yme = yme\n\n"' ''')


abbstat = [['asgardn','asgb'],['aastahansteen','aasta'],['brage','brg'],['draugen','drg'],['deepseaNK','dnk'],['ekofiskL','eko'],['gjoa','gjo'],['goliat','gol'],['grane','gra'],['gudrun','gud'],['gullfaksc','gull'],['heidrun','heid'],['heimdal','heim'],['johansverdrup','josp'],['kvitebjorn','kvit'],['kristin','kris'],['huldra','hul'],['njorda','njo'],['martinlingea','mla'],['martinlingeb','mlb'],['norne','nor'],['ormenlange','orm'],['oseberg','oseb'],['osebergc','osebc'],['osebergsyd','osebs'],['petrojarl','pet'],['scarabeo8','sc8'],['snorrea','snoa'],['snorreb','snob'],['sleipnera','slea'],['sleipnerb','selb'],['statfjorda','stata'],['statfjordb','statb'],['trolla','troa'],['trollb','trob'],['trollc','troc'],['ula','ula'],['valhall','val'],['veslefrikk','vesl'],['visund','vis'],['yme','yme']]

abbstat = np.array(abbstat)

for i in range(0,len(abbstat)):
    textfile.write('''\nalias {}='/vol/vvfelles/printobs/printobs -s {}'  '''.format(abbstat[i,1],abbstat[i,0]))



