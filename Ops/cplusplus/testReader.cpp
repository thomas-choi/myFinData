
#include <iostream>
#include <fstream>
#include <string>
#include "MarketImage.h"

using namespace std;

int main() {
    // Create a file with some data
    // std::ofstream out("data.txt");
    // out << "Hello, world!!!!!!" << std::endl;
    // out.close();

    MarketImage     mi("data.txt");

    // Re-open the file and read its contents
    // std::ifstream in("data.txt");
    // std::string line;
    // std::getline(in, line);
    // in.close();
    string     line = mi.read();
    std::cout << "The original data is: " << line << std::endl;



  char command; // to store the user input command
  string text; // to store the user input text
  string filename = "file.txt"; // the name of the file to write or read
  ofstream outfile; // to write to the file
  ifstream infile; // to read from the file

  cout << "Welcome to the c++ application.\n";
  cout << "Enter 'w' followed by a string to write the string to the file.\n";
  cout << "Enter 'r' to read the file content and display it.\n";
  cout << "Enter 'q' to quit the application.\n";

  while (true) { // loop until the user quits
    cout << "Enter a command: ";
    cin >> command; // get the command from the user

    if (command == 'w') { // if the command is 'w'
        cin >> text; // get the text from the user
        mi.write(text);
    }
    else if (command == 'r') { // if the command is 'r'
        std::cout << "Read MMAP is: " << mi.read() << std::endl;
    }
    else if (command == 'q') { // if the command is 'q'
      cout << "Thank you for using the c++ application. Goodbye!\n";
      break; // exit the loop
    }
    else { // if the command is invalid
      cout << "Invalid command. Please try again.\n";
    }
  }

  return 0;
}
