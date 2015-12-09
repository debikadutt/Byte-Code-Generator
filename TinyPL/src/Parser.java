import java.util.*;
import java.util.HashMap;

//program -> decls stmts end 
//decls -> int idlist ';' 
//idlist -> id [',' idlist ]
//stmts -> stmt [ stmts ]
//stmt -> assign ';'| cmpd | cond | loop
//assign -> id '=' expr 
//cmpd -> '{' stmts '}' 
//cond -> if '(' rexp ')' stmt [ else stmt ]
//loop -> for '(' [assign] ';' [rexp] ';' [assign] ')' stmt 
//rexp -> expr ('<' | '>' | '==' | '!= ') expr 
//expr -> term [ ('+' | '-') expr ] 
//term -> factor [ ('*' | '/') term ] 
//factor -> int_lit | id | '(' expr ')'

public class Parser {

	public static void main(String[] args) {
		System.out.println("Enter an expression, end with semi-colon!\n");
		Lexer.lex();
		
		new Program();
		Code.output();

	}
}

class Program // program -> decls stmts end
{
	Decls d;
	Stmts ss;

	public Program() {
		d = new Decls();
		ss = new Stmts();
		if (Lexer.nextToken == Token.KEY_END) {
			Code.gen(Code.line("return", 1));
		}

	}
}

class Decls // decls -> int idlist ‘;’
{
	Idlist i;
	

	public Decls() {
		
		if (Lexer.nextToken == Token.KEY_INT) {
			Lexer.lex();
			i = new Idlist();
			Lexer.lex();
		}

	}
}

class Idlist // idlist -> id [',' idlist ]
{

	Idlist i;
	char id;

	public Idlist() {
	
		if (Lexer.nextToken == Token.ID) {
		
			id = Lexer.ident;
			Code.idn.add(id);
			Lexer.lex();
		
			if (Lexer.nextToken == Token.COMMA) 
			{
				Lexer.lex();
				i = new Idlist();
			}

		}

	

	}
}

class Stmts // stmts -> stmt [ stmts ]
{
	Stmt s;
	Stmts ss;

	public Stmts() {
		s = new Stmt();
		if (Lexer.nextToken == Token.KEY_END) {
			return;
		}
		if (Lexer.nextToken != Token.RIGHT_BRACE) {
			ss = new Stmts();

		}

	}
}

class Stmt // stmt -> assign ';'| cmpd | cond | loop
{
	Assign a;
	Cmpd c;
	Cond con;
	Loop l;

	public Stmt() {
		
		switch (Lexer.nextToken) {
		case Token.ID:

			a = new Assign();
			Lexer.lex();

			break;
		case Token.KEY_IF:
			con = new Cond();
			break;
		case Token.LEFT_BRACE:
			c = new Cmpd();
			break;
		case Token.KEY_FOR:
			l = new Loop();
			
			break;
		default:
			break;
		}
	}
}

class Assign // assign -> id '=' expr
{
	Expr e;
	char id;

	public Assign() 
	{
		
		id = Lexer.ident;
		if (Lexer.nextToken == Token.ID) {
			Lexer.lex();
			if (Lexer.nextToken == Token.ASSIGN_OP) {
				Lexer.lex();
				e = new Expr();
				Code.gen(Code.store(id));

			}
		}
	}
}

class Cmpd // cmpd -> '{' stmts '}'
{
	Stmts s;

	public Cmpd() {
		if (Lexer.nextToken == Token.LEFT_BRACE) {
			Lexer.lex();
			s = new Stmts();
			Lexer.lex(); // skip over ‘}’
		}
	}

}

class Cond // cond -> if '(' rexp ')' stmt [ else stmt ]
{
	Rexp r;
	Stmt s1, s2;

	public Cond() {
		if (Lexer.nextToken == Token.KEY_IF) {
			Lexer.lex();
			if (Lexer.nextToken == Token.LEFT_PAREN) {
				Lexer.lex();
				r = new Rexp();
				Lexer.lex();
				final int i = Code.ifStatement(r.op);

				s1 = new Stmt();
				if (Lexer.nextToken == Token.KEY_ELSE) {

					int j = Code.gotostatement();
					Code.code[i] = Code.code[i] + Code.linenum;
					Lexer.lex();
					s2 = new Stmt();
					Code.code[j] = Code.code[j] + Code.linenum;

				} else {
					Code.code[i] = Code.code[i] + Code.linenum;
				}

			}
		}
	}
}

class Loop {
	Assign as1;
	Rexp r;
	Assign as2;
	Stmt st;

	public Loop() {
		if (Lexer.nextToken == Token.KEY_FOR) {
			Lexer.lex();
			if (Lexer.nextToken == Token.LEFT_PAREN) {
				Lexer.lex();

				if (Lexer.nextToken == Token.ID) {
					as1 = new Assign();
				}
				Lexer.lex();
				int gt = Code.linenum;

				if (Lexer.nextToken != Token.SEMICOLON) {
					r = new Rexp();
				}
				int ti = -1;
				if (r != null && r.op != null) {
					ti = Code.ifStatement(r.op);
				}
				Lexer.lex();

				int inc = Code.codeptr;
				if (Lexer.nextToken == Token.ID) {
					as2 = new Assign();
				}
				String[] c = new String[30];
				if (as2 != null && as2.e != null) {
					int i = 0;
					for (int j = inc; j < Code.codeptr; j++) {
						c[i] = Code.code[j];
						i++;
					}
				}

				if (Lexer.nextToken == Token.RIGHT_PAREN) {
					Lexer.lex();

					st = new Stmt();

					if (c.length != 0) {
						for (int k = 0; k < c.length; k++) {
							String ins = c[k];
							int len = 1;
							if (k + 1 < c.length && c[k + 1] != null) {
								len = Integer.valueOf(c[k + 1].substring(0, 2)) - Integer.valueOf(ins.substring(0, 2));
							}
							if (ins != null) {
								Code.gen(Code.line(ins.substring(4), len));
							}
						}
					}

					Code.gen(Code.line("goto " + gt, 2));
					if (ti != -1) {
						Code.code[ti] = Code.code[ti] + Code.linenum;
					}
				}
			}
		}
	}
}

class Rexp // rexp -> expr ('<' | '>' | '==' | '!= ') expr
{
	Expr le, re;
	String op;

	public Rexp() {
		le = new Expr();
		switch (Lexer.nextToken) {
		case Token.LESSER_OP:
			op = "<";
			break;
		case Token.GREATER_OP:
			op = ">";
			break;
		case Token.EQ_OP:
			op = "==";
			break;
		case Token.NOT_EQ:
			op = "!=";
			break;
		default:
			break;
		}
		Lexer.lex();
		re = new Expr();
	}

}

class Expr // expr -> term [ ('+' | '-') expr ]
{
	Term t;
	Expr e;
	char op;

	public Expr() {

		t = new Term();
		{
			if (Lexer.nextToken == Token.ADD_OP || Lexer.nextToken == Token.SUB_OP) {
				op = Lexer.nextChar;
				Lexer.lex();
				e = new Expr();
				Code.gen(Code.opcode(op));
			}
		}
	}
}

class Term // term -> factor [ ('*' | '/') term ]
{
	Factor f;
	Term t;
	char op;

	public Term() {
		f = new Factor();
		{
			if (Lexer.nextToken == Token.MULT_OP || Lexer.nextToken == Token.DIV_OP) {
				op = Lexer.nextChar;
				Lexer.lex();
				t = new Term();
				Code.gen(Code.opcode(op));
			}

		}
	}
}

class Factor // factor -> int_lit | id | '(' expr ')'
{
	Expr e;
	int i;
	char s;

	public Factor() {

		switch (Lexer.nextToken) {

		case Token.INT_LIT:

			i = Lexer.intValue;

			Code.gen(Code.intcode(i));
			Lexer.lex();
			break;
		case Token.ID:
			Code.gen(Code.loading(Lexer.ident));
			s = Lexer.ident;
			Lexer.lex();
			break;
		case Token.LEFT_PAREN: // '('
			Lexer.lex();
			e = new Expr();
			Lexer.lex(); // skip over ')'
			break; 

		default:
			break;
		}
	}
}

class Code {
	static String[] code = new String[100];
	static int codeptr = 0;
	static List<Character> idn = new ArrayList<Character>();
	static int linenum = 0;

	public static void gen(String s) {
		code[codeptr] = s;
		codeptr++;
	}

	public static String line(String ins, int length) {
		String s = linenum + ": " + ins;
		linenum += length;
		return s;
	}

	public static String loading(char ident) {

		int found = 0;
		for (int i = 0; i < idn.size(); i++) {
			Character ch = idn.get(i);
			if (ch == ident) {
				found = i;
			}
		}
		found = ++found;
		if (found > 3) {
			return line("iload " + found, 2);

		} else {

			return line("iload_" + found, 1);
		}

	}

	public static String store(char id) {
		int found = 0;
		for (int i = 0; i < idn.size(); i++) {
			Character ch = idn.get(i);
			if (ch == id) {
				found = i;
			}
		}

		found = ++found;
		if (found > 3) {
			return line("istore " + found, 2);

		} else {
			return line("istore_" + found, 1);
		}

	}

	public static String intcode(int i) {
		if (i > 127)
			return line("sipush " + i, 3);
		if (i > 5)
			return line("bipush " + i, 2);
		return line("iconst_" + i, 1);
	}

	public static int ifStatement(String op) {
		int i = codeptr;
		switch (op) {
		case "<":
			gen(line("if_icmpge ", 3));
			break;
		case ">":
			gen(line("if_icmple ", 3));
			break;
		case "==":
			gen(line("if_icmpne ", 3));
			break;
		case "!=":
			gen(line("if_icmpeq ", 3));
			break;
		default:
			break;
		}
		return i;
	}

	public static int gotostatement() {
		int i = codeptr;
		gen(line("goto ", 3));
		return i;
	}

	public static String opcode(char op) {
		switch (op) {
		case '+':
			return line("iadd", 1);
		case '-':
			return line("isub", 1);
		case '*':
			return line("imul", 1);
		case '/':
			return line("idiv", 1);
		default:
			return "";
		}
	}

	public static void output() {
		for (int i = 0; i < codeptr; i++)
			System.out.println(code[i]);
	}
}
