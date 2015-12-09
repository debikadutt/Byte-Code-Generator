class Token {   
public static final int SEMICOLON = 0; 
public static final int COMMA = 1;
public static final int ADD_OP    = 2;   
public static final int SUB_OP    = 3;   
public static final int MULT_OP   = 4;   
public static final int DIV_OP    = 5;   
public static final int ASSIGN_OP  = 6;   
public static final int GREATER_OP = 7;
public static final int LESSER_OP = 8;
public static final int EQ_OP    = 9;     
public static final int NOT_EQ    = 10;     
public static final int LEFT_PAREN= 11;   
public static final int RIGHT_PAREN= 12;   
public static final int LEFT_BRACE= 13;   
public static final int RIGHT_BRACE= 14;   
public static final int ID = 15;   
public static final int INT_LIT  = 16;
public static final int KEY_IF = 17;
public static final int KEY_INT = 18;
public static final int KEY_ELSE = 19;
public static final int KEY_FOR = 20;
public static final int KEY_END = 21;
  
private static String[] lexemes = {   
    ";", ",", "+", "-", "*", "/", "=", ">", "<", "==", "!=",  "(", ")", "{", "}", "id", "int_lit", "if", "int", "else", "for", "end"
    };   
  
public static String toString (int i) {   
    if (i < 0 || i > 21)   
       return "";   
    else return lexemes[i];   
}
} 
