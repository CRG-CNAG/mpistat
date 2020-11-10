// standard headers
#include <cstring>

// return point in the item buf to print to file
// to separate the suffix.
// if there is no suffix set to the end of the buf so it prints nothing
// will ignore a leading dot (hidden files)
// will always print the last suffix.
// if there is a last but one suffix wil print it as well
// if it's length is < 10
char *suffix(char* fname) {
    // get the end of the string
    char *end = strrchr(fname,'\0');

    // work backwards looking for the first dot
    char *test=end;
    bool found_dot = false;
    while ((test--) >= fname) {
        if (*test == '.') {
            found_dot = true;
            break;
        }
    }
    if (!found_dot) {
        return end;
    }

    if (test == fname) {
        // starts with dot so still no suffix
        return end;
    }

    // save position of last dot
    char *last_dot = test;

    // now see if there is a second dot
    found_dot = false;
    while ((test--) >= fname) {
        if (*test == '.') {
            found_dot = true;
            break;
        }
    }

    if (!found_dot) { // only one dot
        return last_dot+1;
    }

    if (test == fname) { // found next dot at start
       return end;
    }

    // if the length of the second suffix is <10 then return a double suffix
    // otherwise just return the first one
    if ((last_dot - test) < 11) {
        return test+1;
    } else {
        return last_dot+1;
    }
}
