/* Driver template for the LEMON parser generator.
** The author disclaims copyright to this source code.
*/
/* First off, code is included that follows the "include" declaration
** in the input grammar file. */
#include <stdio.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>
#include "qSqlparser.h"
#include "tcmdtype.h"
#include "tstoken.h"
#include "ttokendef.h"
#include "tutil.h"
#include "tvariant.h"
/* Next is all token values, in a form suitable for use by makeheaders.
** This section will be null unless lemon is run with the -m switch.
*/
/* 
** These constants (all generated automatically by the parser generator)
** specify the various kinds of tokens (terminals) that the parser
** understands. 
**
** Each symbol here is a terminal symbol in the grammar.
*/
/* Make sure the INTERFACE macro is defined.
*/
#ifndef INTERFACE
# define INTERFACE 1
#endif
/* The next thing included is series of defines which control
** various aspects of the generated parser.
**    YYCODETYPE         is the data type used for storing terminal
**                       and nonterminal numbers.  "unsigned char" is
**                       used if there are fewer than 250 terminals
**                       and nonterminals.  "int" is used otherwise.
**    YYNOCODE           is a number of type YYCODETYPE which corresponds
**                       to no legal terminal or nonterminal number.  This
**                       number is used to fill in empty slots of the hash 
**                       table.
**    YYFALLBACK         If defined, this indicates that one or more tokens
**                       have fall-back values which should be used if the
**                       original value of the token will not parse.
**    YYACTIONTYPE       is the data type used for storing terminal
**                       and nonterminal numbers.  "unsigned char" is
**                       used if there are fewer than 250 rules and
**                       states combined.  "int" is used otherwise.
**    ParseTOKENTYPE     is the data type used for minor tokens given 
**                       directly to the parser from the tokenizer.
**    YYMINORTYPE        is the data type used for all minor tokens.
**                       This is typically a union of many types, one of
**                       which is ParseTOKENTYPE.  The entry in the union
**                       for base tokens is called "yy0".
**    YYSTACKDEPTH       is the maximum depth of the parser's stack.  If
**                       zero the stack is dynamically sized using realloc()
**    ParseARG_SDECL     A static variable declaration for the %extra_argument
**    ParseARG_PDECL     A parameter declaration for the %extra_argument
**    ParseARG_STORE     Code to store %extra_argument into yypParser
**    ParseARG_FETCH     Code to extract %extra_argument from yypParser
**    YYNSTATE           the combined number of states.
**    YYNRULE            the number of rules in the grammar
**    YYERRORSYMBOL      is the code number of the error symbol.  If not
**                       defined, then do no error processing.
*/
#define YYCODETYPE unsigned short int
#define YYNOCODE 274
#define YYACTIONTYPE unsigned short int
#define ParseTOKENTYPE SStrToken
typedef union {
  int yyinit;
  ParseTOKENTYPE yy0;
  int yy46;
  tSQLExpr* yy64;
  tVariant yy134;
  SCreateAcctSQL yy149;
  SDelSQL* yy175;
  int64_t yy207;
  SLimitVal yy216;
  TAOS_FIELD yy223;
  SSubclauseInfo* yy231;
  SCreateDBInfo yy268;
  tSQLExprList* yy290;
  SQuerySQL* yy414;
  SCreateTableSQL* yy470;
  tVariantList* yy498;
  tFieldList* yy523;
  SIntervalVal yy532;
} YYMINORTYPE;
#ifndef YYSTACKDEPTH
#define YYSTACKDEPTH 100
#endif
#define ParseARG_SDECL SSqlInfo* pInfo;
#define ParseARG_PDECL ,SSqlInfo* pInfo
#define ParseARG_FETCH SSqlInfo* pInfo = yypParser->pInfo
#define ParseARG_STORE yypParser->pInfo = pInfo
#define YYNSTATE 420
#define YYNRULE 232
#define YYFALLBACK 1
#define YY_NO_ACTION      (YYNSTATE+YYNRULE+2)
#define YY_ACCEPT_ACTION  (YYNSTATE+YYNRULE+1)
#define YY_ERROR_ACTION   (YYNSTATE+YYNRULE)

/* The yyzerominor constant is used to initialize instances of
** YYMINORTYPE objects to zero. */
static const YYMINORTYPE yyzerominor = { 0 };

/* Define the yytestcase() macro to be a no-op if is not already defined
** otherwise.
**
** Applications can choose to define yytestcase() in the %include section
** to a macro that can assist in verifying code coverage.  For production
** code the yytestcase() macro should be turned off.  But it is useful
** for testing.
*/
#ifndef yytestcase
# define yytestcase(X)
#endif


/* Next are the tables used to determine what action to take based on the
** current state and lookahead token.  These tables are used to implement
** functions that take a state number and lookahead value and return an
** action integer.  
**
** Suppose the action integer is N.  Then the action is determined as
** follows
**
**   0 <= N < YYNSTATE                  Shift N.  That is, push the lookahead
**                                      token onto the stack and goto state N.
**
**   YYNSTATE <= N < YYNSTATE+YYNRULE   Reduce by rule N-YYNSTATE.
**
**   N == YYNSTATE+YYNRULE              A syntax error has occurred.
**
**   N == YYNSTATE+YYNRULE+1            The parser accepts its input.
**
**   N == YYNSTATE+YYNRULE+2            No such action.  Denotes unused
**                                      slots in the yy_action[] table.
**
** The action table is constructed as a single large table named yy_action[].
** Given state S and lookahead X, the action is computed as
**
**      yy_action[ yy_shift_ofst[S] + X ]
**
** If the index value yy_shift_ofst[S]+X is out of range or if the value
** yy_lookahead[yy_shift_ofst[S]+X] is not equal to X or if yy_shift_ofst[S]
** is equal to YY_SHIFT_USE_DFLT, it means that the action is not in the table
** and that yy_default[S] should be used instead.  
**
** The formula above is for computing the action when the lookahead is
** a terminal symbol.  If the lookahead is a non-terminal (as occurs after
** a reduce action) then the yy_reduce_ofst[] array is used in place of
** the yy_shift_ofst[] array and YY_REDUCE_USE_DFLT is used in place of
** YY_SHIFT_USE_DFLT.
**
** The following are the tables generated in this section:
**
**  yy_action[]        A single table containing all actions.
**  yy_lookahead[]     A table containing the lookahead for each entry in
**                     yy_action.  Used to detect hash collisions.
**  yy_shift_ofst[]    For each state, the offset into yy_action for
**                     shifting terminals.
**  yy_reduce_ofst[]   For each state, the offset into yy_action for
**                     shifting non-terminals after a reduce.
**  yy_default[]       Default action for each state.
*/
#define YY_ACTTAB_COUNT (676)
static const YYACTIONTYPE yy_action[] = {
 /*     0 */   404,   34,   33,   76,   80,   32,   31,   30,  403,   85,
 /*    10 */    88,   79,   35,   36,  186,   37,   38,   82,  407,  172,
 /*    20 */    29,  190,  189,  208,   41,   39,   43,   40,  250,  249,
 /*    30 */    96,    8,   34,   33,   63,  120,   32,   31,   30,   35,
 /*    40 */    36,  420,   37,   38,  101,   99,  172,   29,  653,  253,
 /*    50 */   208,   41,   39,   43,   40,   32,   31,   30,   98,   34,
 /*    60 */    33,   87,   86,   32,   31,   30,   35,   36,  404,   37,
 /*    70 */    38,  178,  110,  172,   29,   50,  403,  208,   41,   39,
 /*    80 */    43,   40,  347,   10,    9,  261,   34,   33,  262,  110,
 /*    90 */    32,   31,   30,   51,   41,   39,   43,   40,   75,   74,
 /*   100 */   193,   56,   34,   33,  406,  213,   32,   31,   30,   22,
 /*   110 */   229,  228,  419,  418,  417,  416,  415,  414,  413,  412,
 /*   120 */   411,  410,  409,  408,  252,  304,  270,  183,   36,   61,
 /*   130 */    37,   38,   18,  179,  172,   29,  226,  225,  208,   41,
 /*   140 */    39,   43,   40,  379,  205,  378,   60,   34,   33,   47,
 /*   150 */   314,   32,   31,   30,   22,   37,   38,  110,   97,  172,
 /*   160 */    29,   21,   21,  208,   41,   39,   43,   40,  377,   48,
 /*   170 */   376,  313,   34,   33,  110,   46,   32,   31,   30,  317,
 /*   180 */   396,  329,  328,  327,  326,  325,  324,  323,  322,  321,
 /*   190 */   320,  319,  318,   21,  232,  227,  343,  343,   16,  220,
 /*   200 */   246,  245,  219,  218,  217,  244,  216,  243,  242,  241,
 /*   210 */   215,  171,  292,   21,  277,  301,  203,  296,  209,  295,
 /*   220 */    46,  171,  292,   12,  303,  301,  177,  296,  343,  295,
 /*   230 */   300,  278,  299,  170,  137,  135,  298,  374,  297,  155,
 /*   240 */    93,   92,   91,  168,  169,  156,  176,  264,  343,   90,
 /*   250 */    89,  150,   62,  168,  169,  171,  292,  207,  373,  301,
 /*   260 */   302,  296,   27,  295,  381,  360,  143,  384,  133,  383,
 /*   270 */   366,  382,    3,  368,  367,  166,  286,   17,  365,  291,
 /*   280 */   363,  362,  364,   13,  104,   26,  337,  168,  169,  133,
 /*   290 */    16,  105,  246,  245,  287,  180,  181,  244,   13,  243,
 /*   300 */   242,  241,  372,  123,  124,  192,  143,  306,   42,   70,
 /*   310 */    66,   69,  158,  273,  260,  167,  286,   14,   42,   21,
 /*   320 */   293,  274,  195,  272,  404,   46,  102,  107,  282,  281,
 /*   330 */   293,  231,  403,  361,   26,  294,  133,   78,  261,   17,
 /*   340 */   247,  143,  240,  175,  196,  294,  265,   26,  371,  164,
 /*   350 */   370,  285,   42,  162,  349,  161,  251,  369,  359,  375,
 /*   360 */   358,  344,  375,  357,  293,  356,  380,  355,  375,  354,
 /*   370 */   353,  352,   22,   54,  346,  348,  345,   73,   71,  294,
 /*   380 */    68,  336,  335,   45,  334,  221,  333,  332,  331,  330,
 /*   390 */    65,  212,    7,  210,   15,    6,  305,    4,  267,  284,
 /*   400 */     5,   20,  275,  109,   19,  165,  199,  108,  191,  271,
 /*   410 */    55,  258,  257,  195,   59,  256,  188,  255,  187,  185,
 /*   420 */   184,  254,    2,    1,   95,  402,  248,  100,  397,  131,
 /*   430 */    94,  390,  238,  239,  126,  234,  235,  132,  237,  236,
 /*   440 */    26,  312,  130,   77,  233,  157,   28,  128,  222,  129,
 /*   450 */    67,   64,   53,  127,  118,  211,  350,  198,  204,  200,
 /*   460 */    44,  202,  280,  206,   49,  201,  163,  197,   58,   57,
 /*   470 */   138,  405,  401,  654,  400,  654,  399,  398,  136,  395,
 /*   480 */   654,  394,   18,   52,  276,  393,  654,  654,  117,  392,
 /*   490 */   116,  654,  115,  240,  114,  654,  194,  391,  134,  182,
 /*   500 */   113,  103,  112,  654,  111,  263,  389,  119,  311,  654,
 /*   510 */   654,  654,  654,  654,  230,  174,  259,  654,  654,  654,
 /*   520 */   315,  310,  654,  654,  388,   84,   83,  387,  654,   81,
 /*   530 */   386,  140,   23,  654,  654,   25,  351,  385,  125,  342,
 /*   540 */   341,   72,  340,  224,  338,  654,  223,   24,  139,  214,
 /*   550 */   316,  654,  283,  654,  654,  122,  654,  654,  279,  654,
 /*   560 */   654,  654,  654,  654,  654,  654,  654,  654,  654,  654,
 /*   570 */   654,  654,  654,  654,  121,  654,  654,  654,  654,  654,
 /*   580 */   654,  654,  654,  654,  654,  654,  106,  269,  654,  268,
 /*   590 */   654,  654,  654,  654,  654,  266,  654,  654,  654,  654,
 /*   600 */   654,  654,  654,  654,  654,  654,  654,  654,  654,  654,
 /*   610 */   654,  654,  654,  654,  654,  654,  654,  654,  654,  654,
 /*   620 */   654,  654,  654,  654,  654,  654,  654,  654,  654,  309,
 /*   630 */   173,  308,  307,  654,  654,  654,  654,  654,  339,  654,
 /*   640 */   654,  654,  654,  654,  654,  654,  654,  654,  654,  654,
 /*   650 */   654,  654,  654,  654,  144,  151,  141,  152,  154,  153,
 /*   660 */   149,  148,  146,  145,  160,  159,  654,  290,  289,  288,
 /*   670 */   147,  142,  654,  654,  654,   11,
};
static const YYCODETYPE yy_lookahead[] = {
 /*     0 */     1,   33,   34,   61,   62,   37,   38,   39,    9,   67,
 /*    10 */    68,   69,   13,   14,  127,   16,   17,   75,   58,   20,
 /*    20 */    21,  134,  135,   24,   25,   26,   27,   28,   63,   64,
 /*    30 */    65,   98,   33,   34,  101,  102,   37,   38,   39,   13,
 /*    40 */    14,    0,   16,   17,   61,   62,   20,   21,  208,  209,
 /*    50 */    24,   25,   26,   27,   28,   37,   38,   39,   21,   33,
 /*    60 */    34,   73,   74,   37,   38,   39,   13,   14,    1,   16,
 /*    70 */    17,   66,  211,   20,   21,  103,    9,   24,   25,   26,
 /*    80 */    27,   28,    5,  129,  130,  245,   33,   34,  248,  211,
 /*    90 */    37,   38,   39,  121,   25,   26,   27,   28,  129,  130,
 /*   100 */   260,  102,   33,   34,   59,   99,   37,   38,   39,  103,
 /*   110 */    33,   34,   45,   46,   47,   48,   49,   50,   51,   52,
 /*   120 */    53,   54,   55,   56,   57,   99,  103,   60,   14,  268,
 /*   130 */    16,   17,  109,  128,   20,   21,  131,  132,   24,   25,
 /*   140 */    26,   27,   28,    5,  266,    7,  268,   33,   34,  103,
 /*   150 */    99,   37,   38,   39,  103,   16,   17,  211,   21,   20,
 /*   160 */    21,  211,  211,   24,   25,   26,   27,   28,    5,  123,
 /*   170 */     7,   99,   33,   34,  211,  103,   37,   38,   39,  227,
 /*   180 */    77,  229,  230,  231,  232,  233,  234,  235,  236,  237,
 /*   190 */   238,  239,  240,  211,  244,  244,  246,  246,   85,   86,
 /*   200 */    87,   88,   89,   90,   91,   92,   93,   94,   95,   96,
 /*   210 */    97,    1,    2,  211,  268,    5,  270,    7,   15,    9,
 /*   220 */   103,    1,    2,   44,    1,    5,  244,    7,  246,    9,
 /*   230 */     5,  268,    7,   59,   61,   62,    5,    5,    7,   60,
 /*   240 */    67,   68,   69,   33,   34,   66,  244,   37,  246,   70,
 /*   250 */    71,   72,  251,   33,   34,    1,    2,   37,    5,    5,
 /*   260 */    37,    7,  261,    9,    2,  215,  262,    5,  218,    7,
 /*   270 */   227,    9,   98,  230,  231,  271,  272,   98,  235,   99,
 /*   280 */   237,  238,  239,  103,  105,  106,  215,   33,   34,  218,
 /*   290 */    85,  211,   87,   88,   99,   33,   34,   92,  103,   94,
 /*   300 */    95,   96,    5,   61,   62,  126,  262,  104,   98,   67,
 /*   310 */    68,   69,  133,   99,   99,  271,  272,  103,   98,  211,
 /*   320 */   110,   99,  107,   99,    1,  103,   98,  103,  116,  117,
 /*   330 */   110,  211,    9,  215,  106,  125,  218,   73,  245,   98,
 /*   340 */   228,  262,   78,  228,  264,  125,  211,  106,    5,  228,
 /*   350 */     5,  272,   98,  260,  246,  210,  211,    5,    5,  247,
 /*   360 */     5,  241,  247,    5,  110,    5,  104,    5,  247,    5,
 /*   370 */     5,    5,  103,   98,    5,   99,    5,  130,  130,  125,
 /*   380 */    73,   77,    5,   16,    5,   15,    5,    5,    5,    9,
 /*   390 */    73,  100,   98,  100,   98,  115,  104,   98,  263,   99,
 /*   400 */   115,  103,   99,   98,  103,    1,   98,   98,  127,   99,
 /*   410 */   108,   99,   86,  107,  103,    5,    5,    5,  136,    5,
 /*   420 */   136,    5,  214,  217,  213,  212,   76,   59,  212,  222,
 /*   430 */   213,  212,   81,   79,  225,   49,   80,  219,   53,   82,
 /*   440 */   106,  245,  221,   84,   83,  212,  124,  220,   76,  223,
 /*   450 */   216,  216,  212,  224,  250,  212,  226,  111,  118,  112,
 /*   460 */   119,  113,  212,  114,  122,  265,  265,  265,  212,  212,
 /*   470 */   211,  211,  211,  273,  211,  273,  211,  211,  211,  211,
 /*   480 */   273,  211,  109,  120,  110,  211,  273,  273,  252,  211,
 /*   490 */   253,  273,  254,   78,  255,  273,  245,  211,  211,  211,
 /*   500 */   256,  249,  257,  273,  258,  250,  211,  249,  259,  273,
 /*   510 */   273,  273,  273,  273,  242,  242,  245,  273,  273,  273,
 /*   520 */   243,  242,  273,  273,  211,  211,  211,  211,  273,  211,
 /*   530 */   211,  211,  211,  273,  273,  211,  211,  247,  211,  211,
 /*   540 */   211,  211,  211,  211,  211,  273,  211,  211,  211,  211,
 /*   550 */   211,  273,  269,  273,  273,  211,  273,  273,  269,  273,
 /*   560 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   570 */   273,  273,  273,  273,  211,  273,  273,  273,  273,  273,
 /*   580 */   273,  273,  273,  273,  273,  273,  211,  211,  273,  211,
 /*   590 */   273,  273,  273,  273,  273,  211,  273,  273,  273,  273,
 /*   600 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   610 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   620 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  242,
 /*   630 */   242,  242,  242,  273,  273,  273,  273,  273,  247,  273,
 /*   640 */   273,  273,  273,  273,  273,  273,  273,  273,  273,  273,
 /*   650 */   273,  273,  273,  273,  262,  262,  262,  262,  262,  262,
 /*   660 */   262,  262,  262,  262,  262,  262,  273,  262,  262,  262,
 /*   670 */   262,  262,  273,  273,  273,  262,
};
#define YY_SHIFT_USE_DFLT (-114)
#define YY_SHIFT_COUNT (253)
#define YY_SHIFT_MIN   (-113)
#define YY_SHIFT_MAX   (416)
static const short yy_shift_ofst[] = {
 /*     0 */   179,  113,  205,  220,  254,  323,  323,  323,  323,  323,
 /*    10 */   323,   -1,   67,  254,  262,  262,  262,  241,  323,  323,
 /*    20 */   323,  323,  323,  264,  415,  415, -114,  210,  254,  254,
 /*    30 */   254,  254,  254,  254,  254,  254,  254,  254,  254,  254,
 /*    40 */   254,  254,  254,  254,  254,  262,  262,   77,   77,   77,
 /*    50 */    77,   77,   77,  -67,   77,  228,  323,  323,  323,  323,
 /*    60 */   212,  212,   23,  323,  323,  323,  323,  323,  323,  323,
 /*    70 */   323,  323,  323,  323,  323,  323,  323,  323,  323,  323,
 /*    80 */   323,  323,  323,  323,  323,  323,  323,  323,  323,  323,
 /*    90 */   323,  323,  323,  323,  323,  323,  323,  323,  323,  323,
 /*   100 */   323,  323,  334,  322,  373,  368,  368,  374,  374,  374,
 /*   110 */   368,  363,  342,  341,  349,  340,  348,  347,  346,  322,
 /*   120 */   334,  368,  368,  372,  372,  368,  359,  361,  386,  356,
 /*   130 */   357,  385,  351,  354,  368,  350,  368,  350,  368, -114,
 /*   140 */  -114,   26,   53,   53,   53,  114,  139,   69,   69,   69,
 /*   150 */   -58,  -32,  -32,  -32,  -32,  242,  173,    5, -113,   18,
 /*   160 */    18,  -35,  215,  224,  222,  214,  195,  180,  231,  225,
 /*   170 */   223,  174,  203,   46,  -28,   72,   51,    6,  -31,  -46,
 /*   180 */   163,  138,  -12,  -17,  416,  284,  414,  412,  282,  411,
 /*   190 */   410,  326,  281,  306,  312,  302,  311,  310,  309,  404,
 /*   200 */   308,  303,  305,  301,  285,  298,  280,  300,  299,  292,
 /*   210 */   296,  293,  294,  291,  317,  380,  383,  382,  381,  379,
 /*   220 */   377,  304,  370,  307,  367,  248,  247,  269,  371,  369,
 /*   230 */   276,  275,  269,  366,  365,  364,  362,  360,  358,  355,
 /*   240 */   353,  352,  345,  343,  297,  253,  232,  117,  103,  137,
 /*   250 */    37,   45,  -40,   41,
};
#define YY_REDUCE_USE_DFLT (-161)
#define YY_REDUCE_COUNT (140)
#define YY_REDUCE_MIN   (-160)
#define YY_REDUCE_MAX   (413)
static const short yy_reduce_ofst[] = {
 /*     0 */  -160,  -48,   43,   44,    4,  -54, -122,    2,  -18,  -49,
 /*    10 */   -50,  135,  145,   79,  121,  115,  112,   93,   80,  -37,
 /*    20 */  -139,  120,  108,  118,   71,   50,    1,  413,  409,  408,
 /*    30 */   407,  406,  405,  403,  402,  401,  400,  399,  398,  397,
 /*    40 */   396,  395,  394,  393,  392,  391,  290,  390,  389,  388,
 /*    50 */   387,  279,  273,  277,  272,  271,  384,  378,  376,  375,
 /*    60 */   289,  283,  258,  363,  344,  339,  338,  337,  336,  335,
 /*    70 */   333,  332,  331,  330,  329,  328,  327,  325,  324,  321,
 /*    80 */   320,  319,  318,  316,  315,  314,  313,  295,  288,  287,
 /*    90 */   286,  278,  274,  270,  268,  267,  266,  265,  263,  261,
 /*   100 */   260,  259,  251,  255,  252,  257,  256,  202,  201,  200,
 /*   110 */   250,  249,  246,  245,  244,  239,  238,  237,  236,  204,
 /*   120 */   196,  243,  240,  235,  234,  233,  230,  209,  229,  227,
 /*   130 */   226,  221,  207,  218,  219,  217,  216,  211,  213,  206,
 /*   140 */   208,
};
static const YYACTIONTYPE yy_default[] = {
 /*     0 */   652,  471,  460,  641,  641,  652,  652,  652,  652,  652,
 /*    10 */   652,  566,  435,  641,  652,  652,  652,  652,  652,  652,
 /*    20 */   652,  652,  652,  473,  473,  473,  561,  652,  652,  652,
 /*    30 */   652,  652,  652,  652,  652,  652,  652,  652,  652,  652,
 /*    40 */   652,  652,  652,  652,  652,  652,  652,  652,  652,  652,
 /*    50 */   652,  652,  652,  652,  652,  652,  652,  568,  570,  652,
 /*    60 */   588,  588,  559,  652,  652,  652,  652,  652,  652,  652,
 /*    70 */   652,  652,  652,  652,  652,  652,  652,  652,  652,  652,
 /*    80 */   652,  458,  652,  456,  652,  652,  652,  652,  652,  652,
 /*    90 */   652,  652,  652,  652,  652,  652,  445,  652,  652,  652,
 /*   100 */   652,  652,  652,  603,  652,  437,  437,  652,  652,  652,
 /*   110 */   437,  595,  599,  593,  581,  589,  580,  576,  575,  603,
 /*   120 */   652,  437,  437,  468,  468,  437,  489,  487,  485,  477,
 /*   130 */   483,  479,  481,  475,  437,  466,  437,  466,  437,  505,
 /*   140 */   519,  652,  604,  640,  594,  630,  629,  636,  628,  627,
 /*   150 */   652,  623,  624,  626,  625,  652,  652,  652,  652,  632,
 /*   160 */   631,  652,  652,  652,  652,  652,  652,  652,  652,  652,
 /*   170 */   652,  606,  652,  600,  596,  652,  652,  652,  652,  652,
 /*   180 */   652,  652,  652,  652,  652,  652,  652,  652,  652,  652,
 /*   190 */   652,  652,  652,  558,  652,  652,  567,  652,  652,  652,
 /*   200 */   652,  652,  652,  590,  652,  582,  652,  652,  652,  652,
 /*   210 */   652,  652,  652,  533,  652,  652,  652,  652,  652,  652,
 /*   220 */   652,  652,  652,  652,  652,  652,  652,  645,  652,  652,
 /*   230 */   652,  527,  643,  652,  652,  652,  652,  652,  652,  652,
 /*   240 */   652,  652,  652,  652,  652,  652,  652,  492,  652,  443,
 /*   250 */   441,  652,  433,  652,  651,  650,  649,  642,  557,  556,
 /*   260 */   555,  554,  552,  551,  563,  565,  564,  562,  569,  571,
 /*   270 */   560,  574,  573,  578,  577,  579,  572,  592,  591,  584,
 /*   280 */   585,  587,  586,  583,  620,  638,  639,  637,  635,  634,
 /*   290 */   633,  619,  618,  617,  616,  615,  612,  614,  611,  613,
 /*   300 */   610,  609,  608,  607,  605,  622,  621,  602,  601,  598,
 /*   310 */   597,  553,  536,  535,  534,  532,  472,  518,  517,  516,
 /*   320 */   515,  514,  513,  512,  511,  510,  509,  508,  507,  506,
 /*   330 */   504,  500,  498,  497,  496,  493,  467,  470,  469,  648,
 /*   340 */   647,  646,  644,  538,  539,  531,  530,  529,  528,  537,
 /*   350 */   491,  490,  488,  486,  478,  484,  480,  482,  476,  474,
 /*   360 */   462,  461,  526,  525,  524,  523,  522,  521,  520,  503,
 /*   370 */   502,  501,  499,  495,  494,  541,  550,  549,  548,  547,
 /*   380 */   546,  545,  544,  543,  542,  540,  459,  457,  455,  454,
 /*   390 */   453,  452,  451,  450,  449,  448,  465,  447,  446,  444,
 /*   400 */   442,  440,  439,  464,  463,  438,  436,  434,  432,  431,
 /*   410 */   430,  429,  428,  427,  426,  425,  424,  423,  422,  421,
};

/* The next table maps tokens into fallback tokens.  If a construct
** like the following:
** 
**      %fallback ID X Y Z.
**
** appears in the grammar, then ID becomes a fallback token for X, Y,
** and Z.  Whenever one of the tokens X, Y, or Z is input to the parser
** but it does not parse, the type of the token is changed to ID and
** the parse is retried before an error is thrown.
*/
#ifdef YYFALLBACK
static const YYCODETYPE yyFallback[] = {
    0,  /*          $ => nothing */
    0,  /*         ID => nothing */
    1,  /*       BOOL => ID */
    1,  /*    TINYINT => ID */
    1,  /*   SMALLINT => ID */
    1,  /*    INTEGER => ID */
    1,  /*     BIGINT => ID */
    1,  /*      FLOAT => ID */
    1,  /*     DOUBLE => ID */
    1,  /*     STRING => ID */
    1,  /*  TIMESTAMP => ID */
    1,  /*     BINARY => ID */
    1,  /*      NCHAR => ID */
    0,  /*         OR => nothing */
    0,  /*        AND => nothing */
    0,  /*        NOT => nothing */
    0,  /*         EQ => nothing */
    0,  /*         NE => nothing */
    0,  /*     ISNULL => nothing */
    0,  /*    NOTNULL => nothing */
    0,  /*         IS => nothing */
    1,  /*       LIKE => ID */
    1,  /*       GLOB => ID */
    0,  /*    BETWEEN => nothing */
    0,  /*         IN => nothing */
    0,  /*         GT => nothing */
    0,  /*         GE => nothing */
    0,  /*         LT => nothing */
    0,  /*         LE => nothing */
    0,  /*     BITAND => nothing */
    0,  /*      BITOR => nothing */
    0,  /*     LSHIFT => nothing */
    0,  /*     RSHIFT => nothing */
    0,  /*       PLUS => nothing */
    0,  /*      MINUS => nothing */
    0,  /*     DIVIDE => nothing */
    0,  /*      TIMES => nothing */
    0,  /*       STAR => nothing */
    0,  /*      SLASH => nothing */
    0,  /*        REM => nothing */
    0,  /*     CONCAT => nothing */
    0,  /*     UMINUS => nothing */
    0,  /*      UPLUS => nothing */
    0,  /*     BITNOT => nothing */
    0,  /*       SHOW => nothing */
    0,  /*  DATABASES => nothing */
    0,  /*     MNODES => nothing */
    0,  /*     DNODES => nothing */
    0,  /*   ACCOUNTS => nothing */
    0,  /*      USERS => nothing */
    0,  /*    MODULES => nothing */
    0,  /*    QUERIES => nothing */
    0,  /* CONNECTIONS => nothing */
    0,  /*    STREAMS => nothing */
    0,  /*  VARIABLES => nothing */
    0,  /*     SCORES => nothing */
    0,  /*     GRANTS => nothing */
    0,  /*     VNODES => nothing */
    1,  /*    IPTOKEN => ID */
    0,  /*        DOT => nothing */
    0,  /*     CREATE => nothing */
    0,  /*      TABLE => nothing */
    1,  /*   DATABASE => ID */
    0,  /*     TABLES => nothing */
    0,  /*    STABLES => nothing */
    0,  /*    VGROUPS => nothing */
    0,  /*       DROP => nothing */
    0,  /*      DNODE => nothing */
    0,  /*       USER => nothing */
    0,  /*    ACCOUNT => nothing */
    0,  /*        USE => nothing */
    0,  /*   DESCRIBE => nothing */
    0,  /*      ALTER => nothing */
    0,  /*       PASS => nothing */
    0,  /*  PRIVILEGE => nothing */
    0,  /*      LOCAL => nothing */
    0,  /*         IF => nothing */
    0,  /*     EXISTS => nothing */
    0,  /*        PPS => nothing */
    0,  /*    TSERIES => nothing */
    0,  /*        DBS => nothing */
    0,  /*    STORAGE => nothing */
    0,  /*      QTIME => nothing */
    0,  /*      CONNS => nothing */
    0,  /*      STATE => nothing */
    0,  /*       KEEP => nothing */
    0,  /*      CACHE => nothing */
    0,  /*    REPLICA => nothing */
    0,  /*     QUORUM => nothing */
    0,  /*       DAYS => nothing */
    0,  /*    MINROWS => nothing */
    0,  /*    MAXROWS => nothing */
    0,  /*     BLOCKS => nothing */
    0,  /*      CTIME => nothing */
    0,  /*        WAL => nothing */
    0,  /*      FSYNC => nothing */
    0,  /*       COMP => nothing */
    0,  /*  PRECISION => nothing */
    0,  /*         LP => nothing */
    0,  /*         RP => nothing */
    0,  /*       TAGS => nothing */
    0,  /*      USING => nothing */
    0,  /*         AS => nothing */
    0,  /*      COMMA => nothing */
    1,  /*       NULL => ID */
    0,  /*     DELETE => nothing */
    0,  /*     SELECT => nothing */
    0,  /*      UNION => nothing */
    1,  /*        ALL => ID */
    0,  /*       FROM => nothing */
    0,  /*   VARIABLE => nothing */
    0,  /*   INTERVAL => nothing */
    0,  /*       FILL => nothing */
    0,  /*    SLIDING => nothing */
    0,  /*      ORDER => nothing */
    0,  /*         BY => nothing */
    1,  /*        ASC => ID */
    1,  /*       DESC => ID */
    0,  /*      GROUP => nothing */
    0,  /*     HAVING => nothing */
    0,  /*      LIMIT => nothing */
    1,  /*     OFFSET => ID */
    0,  /*     SLIMIT => nothing */
    0,  /*    SOFFSET => nothing */
    0,  /*      WHERE => nothing */
    1,  /*        NOW => ID */
    0,  /*      RESET => nothing */
    0,  /*      QUERY => nothing */
    0,  /*        ADD => nothing */
    0,  /*     COLUMN => nothing */
    0,  /*        TAG => nothing */
    0,  /*     CHANGE => nothing */
    0,  /*        SET => nothing */
    0,  /*       KILL => nothing */
    0,  /* CONNECTION => nothing */
    0,  /*     STREAM => nothing */
    0,  /*      COLON => nothing */
    1,  /*      ABORT => ID */
    1,  /*      AFTER => ID */
    1,  /*     ATTACH => ID */
    1,  /*     BEFORE => ID */
    1,  /*      BEGIN => ID */
    1,  /*    CASCADE => ID */
    1,  /*    CLUSTER => ID */
    1,  /*   CONFLICT => ID */
    1,  /*       COPY => ID */
    1,  /*   DEFERRED => ID */
    1,  /* DELIMITERS => ID */
    1,  /*     DETACH => ID */
    1,  /*       EACH => ID */
    1,  /*        END => ID */
    1,  /*    EXPLAIN => ID */
    1,  /*       FAIL => ID */
    1,  /*        FOR => ID */
    1,  /*     IGNORE => ID */
    1,  /*  IMMEDIATE => ID */
    1,  /*  INITIALLY => ID */
    1,  /*    INSTEAD => ID */
    1,  /*      MATCH => ID */
    1,  /*        KEY => ID */
    1,  /*         OF => ID */
    1,  /*      RAISE => ID */
    1,  /*    REPLACE => ID */
    1,  /*   RESTRICT => ID */
    1,  /*        ROW => ID */
    1,  /*  STATEMENT => ID */
    1,  /*    TRIGGER => ID */
    1,  /*       VIEW => ID */
    1,  /*      COUNT => ID */
    1,  /*        SUM => ID */
    1,  /*        AVG => ID */
    1,  /*        MIN => ID */
    1,  /*        MAX => ID */
    1,  /*      FIRST => ID */
    1,  /*       LAST => ID */
    1,  /*        TOP => ID */
    1,  /*     BOTTOM => ID */
    1,  /*     STDDEV => ID */
    1,  /* PERCENTILE => ID */
    1,  /* APERCENTILE => ID */
    1,  /* LEASTSQUARES => ID */
    1,  /*  HISTOGRAM => ID */
    1,  /*       DIFF => ID */
    1,  /*     SPREAD => ID */
    1,  /*        TWA => ID */
    1,  /*     INTERP => ID */
    1,  /*   LAST_ROW => ID */
    1,  /*       RATE => ID */
    1,  /*      IRATE => ID */
    1,  /*   SUM_RATE => ID */
    1,  /*  SUM_IRATE => ID */
    1,  /*   AVG_RATE => ID */
    1,  /*  AVG_IRATE => ID */
    1,  /*       TBID => ID */
    1,  /*       SEMI => ID */
    1,  /*       NONE => ID */
    1,  /*       PREV => ID */
    1,  /*     LINEAR => ID */
    1,  /*     IMPORT => ID */
    1,  /*     METRIC => ID */
    1,  /*     TBNAME => ID */
    1,  /*       JOIN => ID */
    1,  /*    METRICS => ID */
    1,  /*     STABLE => ID */
    1,  /*     INSERT => ID */
    1,  /*       INTO => ID */
    1,  /*     VALUES => ID */
};
#endif /* YYFALLBACK */

/* The following structure represents a single element of the
** parser's stack.  Information stored includes:
**
**   +  The state number for the parser at this level of the stack.
**
**   +  The value of the token stored at this level of the stack.
**      (In other words, the "major" token.)
**
**   +  The semantic value stored at this level of the stack.  This is
**      the information used by the action routines in the grammar.
**      It is sometimes called the "minor" token.
*/
struct yyStackEntry {
  YYACTIONTYPE stateno;  /* The state-number */
  YYCODETYPE major;      /* The major token value.  This is the code
                         ** number for the token at this stack level */
  YYMINORTYPE minor;     /* The user-supplied minor token value.  This
                         ** is the value of the token  */
};
typedef struct yyStackEntry yyStackEntry;

/* The state of the parser is completely contained in an instance of
** the following structure */
struct yyParser {
  int yyidx;                    /* Index of top element in stack */
#ifdef YYTRACKMAXSTACKDEPTH
  int yyidxMax;                 /* Maximum value of yyidx */
#endif
  int yyerrcnt;                 /* Shifts left before out of the error */
  ParseARG_SDECL                /* A place to hold %extra_argument */
#if YYSTACKDEPTH<=0
  int yystksz;                  /* Current side of the stack */
  yyStackEntry *yystack;        /* The parser's stack */
#else
  yyStackEntry yystack[YYSTACKDEPTH];  /* The parser's stack */
#endif
};
typedef struct yyParser yyParser;

#ifndef NDEBUG
#include <stdio.h>
static FILE *yyTraceFILE = 0;
static char *yyTracePrompt = 0;
#endif /* NDEBUG */

#ifndef NDEBUG
/* 
** Turn parser tracing on by giving a stream to which to write the trace
** and a prompt to preface each trace message.  Tracing is turned off
** by making either argument NULL 
**
** Inputs:
** <ul>
** <li> A FILE* to which trace output should be written.
**      If NULL, then tracing is turned off.
** <li> A prefix string written at the beginning of every
**      line of trace output.  If NULL, then tracing is
**      turned off.
** </ul>
**
** Outputs:
** None.
*/
void ParseTrace(FILE *TraceFILE, char *zTracePrompt){
  yyTraceFILE = TraceFILE;
  yyTracePrompt = zTracePrompt;
  if( yyTraceFILE==0 ) yyTracePrompt = 0;
  else if( yyTracePrompt==0 ) yyTraceFILE = 0;
}
#endif /* NDEBUG */

#ifndef NDEBUG
/* For tracing shifts, the names of all terminals and nonterminals
** are required.  The following table supplies these names */
static const char *const yyTokenName[] = { 
  "$",             "ID",            "BOOL",          "TINYINT",     
  "SMALLINT",      "INTEGER",       "BIGINT",        "FLOAT",       
  "DOUBLE",        "STRING",        "TIMESTAMP",     "BINARY",      
  "NCHAR",         "OR",            "AND",           "NOT",         
  "EQ",            "NE",            "ISNULL",        "NOTNULL",     
  "IS",            "LIKE",          "GLOB",          "BETWEEN",     
  "IN",            "GT",            "GE",            "LT",          
  "LE",            "BITAND",        "BITOR",         "LSHIFT",      
  "RSHIFT",        "PLUS",          "MINUS",         "DIVIDE",      
  "TIMES",         "STAR",          "SLASH",         "REM",         
  "CONCAT",        "UMINUS",        "UPLUS",         "BITNOT",      
  "SHOW",          "DATABASES",     "MNODES",        "DNODES",      
  "ACCOUNTS",      "USERS",         "MODULES",       "QUERIES",     
  "CONNECTIONS",   "STREAMS",       "VARIABLES",     "SCORES",      
  "GRANTS",        "VNODES",        "IPTOKEN",       "DOT",         
  "CREATE",        "TABLE",         "DATABASE",      "TABLES",      
  "STABLES",       "VGROUPS",       "DROP",          "DNODE",       
  "USER",          "ACCOUNT",       "USE",           "DESCRIBE",    
  "ALTER",         "PASS",          "PRIVILEGE",     "LOCAL",       
  "IF",            "EXISTS",        "PPS",           "TSERIES",     
  "DBS",           "STORAGE",       "QTIME",         "CONNS",       
  "STATE",         "KEEP",          "CACHE",         "REPLICA",     
  "QUORUM",        "DAYS",          "MINROWS",       "MAXROWS",     
  "BLOCKS",        "CTIME",         "WAL",           "FSYNC",       
  "COMP",          "PRECISION",     "LP",            "RP",          
  "TAGS",          "USING",         "AS",            "COMMA",       
  "NULL",          "DELETE",        "SELECT",        "UNION",       
  "ALL",           "FROM",          "VARIABLE",      "INTERVAL",    
  "FILL",          "SLIDING",       "ORDER",         "BY",          
  "ASC",           "DESC",          "GROUP",         "HAVING",      
  "LIMIT",         "OFFSET",        "SLIMIT",        "SOFFSET",     
  "WHERE",         "NOW",           "RESET",         "QUERY",       
  "ADD",           "COLUMN",        "TAG",           "CHANGE",      
  "SET",           "KILL",          "CONNECTION",    "STREAM",      
  "COLON",         "ABORT",         "AFTER",         "ATTACH",      
  "BEFORE",        "BEGIN",         "CASCADE",       "CLUSTER",     
  "CONFLICT",      "COPY",          "DEFERRED",      "DELIMITERS",  
  "DETACH",        "EACH",          "END",           "EXPLAIN",     
  "FAIL",          "FOR",           "IGNORE",        "IMMEDIATE",   
  "INITIALLY",     "INSTEAD",       "MATCH",         "KEY",         
  "OF",            "RAISE",         "REPLACE",       "RESTRICT",    
  "ROW",           "STATEMENT",     "TRIGGER",       "VIEW",        
  "COUNT",         "SUM",           "AVG",           "MIN",         
  "MAX",           "FIRST",         "LAST",          "TOP",         
  "BOTTOM",        "STDDEV",        "PERCENTILE",    "APERCENTILE", 
  "LEASTSQUARES",  "HISTOGRAM",     "DIFF",          "SPREAD",      
  "TWA",           "INTERP",        "LAST_ROW",      "RATE",        
  "IRATE",         "SUM_RATE",      "SUM_IRATE",     "AVG_RATE",    
  "AVG_IRATE",     "TBID",          "SEMI",          "NONE",        
  "PREV",          "LINEAR",        "IMPORT",        "METRIC",      
  "TBNAME",        "JOIN",          "METRICS",       "STABLE",      
  "INSERT",        "INTO",          "VALUES",        "error",       
  "program",       "cmd",           "dbPrefix",      "ids",         
  "cpxName",       "ifexists",      "alter_db_optr",  "acct_optr",   
  "ifnotexists",   "db_optr",       "pps",           "tseries",     
  "dbs",           "streams",       "storage",       "qtime",       
  "users",         "conns",         "state",         "keep",        
  "tagitemlist",   "cache",         "replica",       "quorum",      
  "days",          "minrows",       "maxrows",       "blocks",      
  "ctime",         "wal",           "fsync",         "comp",        
  "prec",          "typename",      "signed",        "create_table_args",
  "columnlist",    "select",        "column",        "tagitem",     
  "delete",        "from",          "where_opt",     "selcollist",  
  "interval_opt",  "fill_opt",      "sliding_opt",   "groupby_opt", 
  "orderby_opt",   "having_opt",    "slimit_opt",    "limit_opt",   
  "union",         "sclp",          "expr",          "as",          
  "tablelist",     "tmvar",         "sortlist",      "sortitem",    
  "item",          "sortorder",     "grouplist",     "exprlist",    
  "expritem",    
};
#endif /* NDEBUG */

#ifndef NDEBUG
/* For tracing reduce actions, the names of all rules are required.
*/
static const char *const yyRuleName[] = {
 /*   0 */ "program ::= cmd",
 /*   1 */ "cmd ::= SHOW DATABASES",
 /*   2 */ "cmd ::= SHOW MNODES",
 /*   3 */ "cmd ::= SHOW DNODES",
 /*   4 */ "cmd ::= SHOW ACCOUNTS",
 /*   5 */ "cmd ::= SHOW USERS",
 /*   6 */ "cmd ::= SHOW MODULES",
 /*   7 */ "cmd ::= SHOW QUERIES",
 /*   8 */ "cmd ::= SHOW CONNECTIONS",
 /*   9 */ "cmd ::= SHOW STREAMS",
 /*  10 */ "cmd ::= SHOW VARIABLES",
 /*  11 */ "cmd ::= SHOW SCORES",
 /*  12 */ "cmd ::= SHOW GRANTS",
 /*  13 */ "cmd ::= SHOW VNODES",
 /*  14 */ "cmd ::= SHOW VNODES IPTOKEN",
 /*  15 */ "dbPrefix ::=",
 /*  16 */ "dbPrefix ::= ids DOT",
 /*  17 */ "cpxName ::=",
 /*  18 */ "cpxName ::= DOT ids",
 /*  19 */ "cmd ::= SHOW CREATE TABLE ids cpxName",
 /*  20 */ "cmd ::= SHOW CREATE DATABASE ids",
 /*  21 */ "cmd ::= SHOW dbPrefix TABLES",
 /*  22 */ "cmd ::= SHOW dbPrefix TABLES LIKE ids",
 /*  23 */ "cmd ::= SHOW dbPrefix STABLES",
 /*  24 */ "cmd ::= SHOW dbPrefix STABLES LIKE ids",
 /*  25 */ "cmd ::= SHOW dbPrefix VGROUPS",
 /*  26 */ "cmd ::= SHOW dbPrefix VGROUPS ids",
 /*  27 */ "cmd ::= DROP TABLE ifexists ids cpxName",
 /*  28 */ "cmd ::= DROP DATABASE ifexists ids",
 /*  29 */ "cmd ::= DROP DNODE ids",
 /*  30 */ "cmd ::= DROP USER ids",
 /*  31 */ "cmd ::= DROP ACCOUNT ids",
 /*  32 */ "cmd ::= USE ids",
 /*  33 */ "cmd ::= DESCRIBE ids cpxName",
 /*  34 */ "cmd ::= ALTER USER ids PASS ids",
 /*  35 */ "cmd ::= ALTER USER ids PRIVILEGE ids",
 /*  36 */ "cmd ::= ALTER DNODE ids ids",
 /*  37 */ "cmd ::= ALTER DNODE ids ids ids",
 /*  38 */ "cmd ::= ALTER LOCAL ids",
 /*  39 */ "cmd ::= ALTER LOCAL ids ids",
 /*  40 */ "cmd ::= ALTER DATABASE ids alter_db_optr",
 /*  41 */ "cmd ::= ALTER ACCOUNT ids acct_optr",
 /*  42 */ "cmd ::= ALTER ACCOUNT ids PASS ids acct_optr",
 /*  43 */ "ids ::= ID",
 /*  44 */ "ids ::= STRING",
 /*  45 */ "ifexists ::= IF EXISTS",
 /*  46 */ "ifexists ::=",
 /*  47 */ "ifnotexists ::= IF NOT EXISTS",
 /*  48 */ "ifnotexists ::=",
 /*  49 */ "cmd ::= CREATE DNODE ids",
 /*  50 */ "cmd ::= CREATE ACCOUNT ids PASS ids acct_optr",
 /*  51 */ "cmd ::= CREATE DATABASE ifnotexists ids db_optr",
 /*  52 */ "cmd ::= CREATE USER ids PASS ids",
 /*  53 */ "pps ::=",
 /*  54 */ "pps ::= PPS INTEGER",
 /*  55 */ "tseries ::=",
 /*  56 */ "tseries ::= TSERIES INTEGER",
 /*  57 */ "dbs ::=",
 /*  58 */ "dbs ::= DBS INTEGER",
 /*  59 */ "streams ::=",
 /*  60 */ "streams ::= STREAMS INTEGER",
 /*  61 */ "storage ::=",
 /*  62 */ "storage ::= STORAGE INTEGER",
 /*  63 */ "qtime ::=",
 /*  64 */ "qtime ::= QTIME INTEGER",
 /*  65 */ "users ::=",
 /*  66 */ "users ::= USERS INTEGER",
 /*  67 */ "conns ::=",
 /*  68 */ "conns ::= CONNS INTEGER",
 /*  69 */ "state ::=",
 /*  70 */ "state ::= STATE ids",
 /*  71 */ "acct_optr ::= pps tseries storage streams qtime dbs users conns state",
 /*  72 */ "keep ::= KEEP tagitemlist",
 /*  73 */ "cache ::= CACHE INTEGER",
 /*  74 */ "replica ::= REPLICA INTEGER",
 /*  75 */ "quorum ::= QUORUM INTEGER",
 /*  76 */ "days ::= DAYS INTEGER",
 /*  77 */ "minrows ::= MINROWS INTEGER",
 /*  78 */ "maxrows ::= MAXROWS INTEGER",
 /*  79 */ "blocks ::= BLOCKS INTEGER",
 /*  80 */ "ctime ::= CTIME INTEGER",
 /*  81 */ "wal ::= WAL INTEGER",
 /*  82 */ "fsync ::= FSYNC INTEGER",
 /*  83 */ "comp ::= COMP INTEGER",
 /*  84 */ "prec ::= PRECISION STRING",
 /*  85 */ "db_optr ::=",
 /*  86 */ "db_optr ::= db_optr cache",
 /*  87 */ "db_optr ::= db_optr replica",
 /*  88 */ "db_optr ::= db_optr quorum",
 /*  89 */ "db_optr ::= db_optr days",
 /*  90 */ "db_optr ::= db_optr minrows",
 /*  91 */ "db_optr ::= db_optr maxrows",
 /*  92 */ "db_optr ::= db_optr blocks",
 /*  93 */ "db_optr ::= db_optr ctime",
 /*  94 */ "db_optr ::= db_optr wal",
 /*  95 */ "db_optr ::= db_optr fsync",
 /*  96 */ "db_optr ::= db_optr comp",
 /*  97 */ "db_optr ::= db_optr prec",
 /*  98 */ "db_optr ::= db_optr keep",
 /*  99 */ "alter_db_optr ::=",
 /* 100 */ "alter_db_optr ::= alter_db_optr replica",
 /* 101 */ "alter_db_optr ::= alter_db_optr quorum",
 /* 102 */ "alter_db_optr ::= alter_db_optr keep",
 /* 103 */ "alter_db_optr ::= alter_db_optr blocks",
 /* 104 */ "alter_db_optr ::= alter_db_optr comp",
 /* 105 */ "alter_db_optr ::= alter_db_optr wal",
 /* 106 */ "alter_db_optr ::= alter_db_optr fsync",
 /* 107 */ "typename ::= ids",
 /* 108 */ "typename ::= ids LP signed RP",
 /* 109 */ "signed ::= INTEGER",
 /* 110 */ "signed ::= PLUS INTEGER",
 /* 111 */ "signed ::= MINUS INTEGER",
 /* 112 */ "cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args",
 /* 113 */ "create_table_args ::= LP columnlist RP",
 /* 114 */ "create_table_args ::= LP columnlist RP TAGS LP columnlist RP",
 /* 115 */ "create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP",
 /* 116 */ "create_table_args ::= AS select",
 /* 117 */ "columnlist ::= columnlist COMMA column",
 /* 118 */ "columnlist ::= column",
 /* 119 */ "column ::= ids typename",
 /* 120 */ "tagitemlist ::= tagitemlist COMMA tagitem",
 /* 121 */ "tagitemlist ::= tagitem",
 /* 122 */ "tagitem ::= INTEGER",
 /* 123 */ "tagitem ::= FLOAT",
 /* 124 */ "tagitem ::= STRING",
 /* 125 */ "tagitem ::= BOOL",
 /* 126 */ "tagitem ::= NULL",
 /* 127 */ "tagitem ::= MINUS INTEGER",
 /* 128 */ "tagitem ::= MINUS FLOAT",
 /* 129 */ "tagitem ::= PLUS INTEGER",
 /* 130 */ "tagitem ::= PLUS FLOAT",
 /* 131 */ "delete ::= DELETE from where_opt",
 /* 132 */ "cmd ::= delete",
 /* 133 */ "select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt",
 /* 134 */ "union ::= select",
 /* 135 */ "union ::= LP union RP",
 /* 136 */ "union ::= union UNION ALL select",
 /* 137 */ "union ::= union UNION ALL LP select RP",
 /* 138 */ "cmd ::= union",
 /* 139 */ "select ::= SELECT selcollist",
 /* 140 */ "sclp ::= selcollist COMMA",
 /* 141 */ "sclp ::=",
 /* 142 */ "selcollist ::= sclp expr as",
 /* 143 */ "selcollist ::= sclp STAR",
 /* 144 */ "as ::= AS ids",
 /* 145 */ "as ::= ids",
 /* 146 */ "as ::=",
 /* 147 */ "from ::= FROM tablelist",
 /* 148 */ "tablelist ::= ids cpxName",
 /* 149 */ "tablelist ::= ids cpxName ids",
 /* 150 */ "tablelist ::= tablelist COMMA ids cpxName",
 /* 151 */ "tablelist ::= tablelist COMMA ids cpxName ids",
 /* 152 */ "tmvar ::= VARIABLE",
 /* 153 */ "interval_opt ::= INTERVAL LP tmvar RP",
 /* 154 */ "interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP",
 /* 155 */ "interval_opt ::=",
 /* 156 */ "fill_opt ::=",
 /* 157 */ "fill_opt ::= FILL LP ID COMMA tagitemlist RP",
 /* 158 */ "fill_opt ::= FILL LP ID RP",
 /* 159 */ "sliding_opt ::= SLIDING LP tmvar RP",
 /* 160 */ "sliding_opt ::=",
 /* 161 */ "orderby_opt ::=",
 /* 162 */ "orderby_opt ::= ORDER BY sortlist",
 /* 163 */ "sortlist ::= sortlist COMMA item sortorder",
 /* 164 */ "sortlist ::= item sortorder",
 /* 165 */ "item ::= ids cpxName",
 /* 166 */ "sortorder ::= ASC",
 /* 167 */ "sortorder ::= DESC",
 /* 168 */ "sortorder ::=",
 /* 169 */ "groupby_opt ::=",
 /* 170 */ "groupby_opt ::= GROUP BY grouplist",
 /* 171 */ "grouplist ::= grouplist COMMA item",
 /* 172 */ "grouplist ::= item",
 /* 173 */ "having_opt ::=",
 /* 174 */ "having_opt ::= HAVING expr",
 /* 175 */ "limit_opt ::=",
 /* 176 */ "limit_opt ::= LIMIT signed",
 /* 177 */ "limit_opt ::= LIMIT signed OFFSET signed",
 /* 178 */ "limit_opt ::= LIMIT signed COMMA signed",
 /* 179 */ "slimit_opt ::=",
 /* 180 */ "slimit_opt ::= SLIMIT signed",
 /* 181 */ "slimit_opt ::= SLIMIT signed SOFFSET signed",
 /* 182 */ "slimit_opt ::= SLIMIT signed COMMA signed",
 /* 183 */ "where_opt ::=",
 /* 184 */ "where_opt ::= WHERE expr",
 /* 185 */ "expr ::= LP expr RP",
 /* 186 */ "expr ::= ID",
 /* 187 */ "expr ::= ID DOT ID",
 /* 188 */ "expr ::= ID DOT STAR",
 /* 189 */ "expr ::= INTEGER",
 /* 190 */ "expr ::= MINUS INTEGER",
 /* 191 */ "expr ::= PLUS INTEGER",
 /* 192 */ "expr ::= FLOAT",
 /* 193 */ "expr ::= MINUS FLOAT",
 /* 194 */ "expr ::= PLUS FLOAT",
 /* 195 */ "expr ::= STRING",
 /* 196 */ "expr ::= NOW",
 /* 197 */ "expr ::= VARIABLE",
 /* 198 */ "expr ::= BOOL",
 /* 199 */ "expr ::= ID LP exprlist RP",
 /* 200 */ "expr ::= ID LP STAR RP",
 /* 201 */ "expr ::= expr IS NULL",
 /* 202 */ "expr ::= expr IS NOT NULL",
 /* 203 */ "expr ::= expr LT expr",
 /* 204 */ "expr ::= expr GT expr",
 /* 205 */ "expr ::= expr LE expr",
 /* 206 */ "expr ::= expr GE expr",
 /* 207 */ "expr ::= expr NE expr",
 /* 208 */ "expr ::= expr EQ expr",
 /* 209 */ "expr ::= expr AND expr",
 /* 210 */ "expr ::= expr OR expr",
 /* 211 */ "expr ::= expr PLUS expr",
 /* 212 */ "expr ::= expr MINUS expr",
 /* 213 */ "expr ::= expr STAR expr",
 /* 214 */ "expr ::= expr SLASH expr",
 /* 215 */ "expr ::= expr REM expr",
 /* 216 */ "expr ::= expr LIKE expr",
 /* 217 */ "expr ::= expr IN LP exprlist RP",
 /* 218 */ "exprlist ::= exprlist COMMA expritem",
 /* 219 */ "exprlist ::= expritem",
 /* 220 */ "expritem ::= expr",
 /* 221 */ "expritem ::=",
 /* 222 */ "cmd ::= RESET QUERY CACHE",
 /* 223 */ "cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist",
 /* 224 */ "cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids",
 /* 225 */ "cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist",
 /* 226 */ "cmd ::= ALTER TABLE ids cpxName DROP TAG ids",
 /* 227 */ "cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids",
 /* 228 */ "cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem",
 /* 229 */ "cmd ::= KILL CONNECTION INTEGER",
 /* 230 */ "cmd ::= KILL STREAM INTEGER COLON INTEGER",
 /* 231 */ "cmd ::= KILL QUERY INTEGER COLON INTEGER",
};
#endif /* NDEBUG */


#if YYSTACKDEPTH<=0
/*
** Try to increase the size of the parser stack.
*/
static void yyGrowStack(yyParser *p){
  int newSize;
  yyStackEntry *pNew;

  newSize = p->yystksz*2 + 100;
  pNew = realloc(p->yystack, newSize*sizeof(pNew[0]));
  if( pNew ){
    p->yystack = pNew;
    p->yystksz = newSize;
#ifndef NDEBUG
    if( yyTraceFILE ){
      fprintf(yyTraceFILE,"%sStack grows to %d entries!\n",
              yyTracePrompt, p->yystksz);
    }
#endif
  }
}
#endif

/* 
** This function allocates a new parser.
** The only argument is a pointer to a function which works like
** malloc.
**
** Inputs:
** A pointer to the function used to allocate memory.
**
** Outputs:
** A pointer to a parser.  This pointer is used in subsequent calls
** to Parse and ParseFree.
*/
void *ParseAlloc(void *(*mallocProc)(size_t)){
  yyParser *pParser;
  pParser = (yyParser*)(*mallocProc)( (size_t)sizeof(yyParser) );
  if( pParser ){
    pParser->yyidx = -1;
#ifdef YYTRACKMAXSTACKDEPTH
    pParser->yyidxMax = 0;
#endif
#if YYSTACKDEPTH<=0
    pParser->yystack = NULL;
    pParser->yystksz = 0;
    yyGrowStack(pParser);
#endif
  }
  return pParser;
}

/* The following function deletes the value associated with a
** symbol.  The symbol can be either a terminal or nonterminal.
** "yymajor" is the symbol code, and "yypminor" is a pointer to
** the value.
*/
static void yy_destructor(
  yyParser *yypParser,    /* The parser */
  YYCODETYPE yymajor,     /* Type code for object to destroy */
  YYMINORTYPE *yypminor   /* The object to be destroyed */
){
  ParseARG_FETCH;
  switch( yymajor ){
    /* Here is inserted the actions which take place when a
    ** terminal or non-terminal is destroyed.  This can happen
    ** when the symbol is popped from the stack during a
    ** reduce or during error processing or when a parser is 
    ** being destroyed before it is finished parsing.
    **
    ** Note: during a reduce, the only symbols destroyed are those
    ** which appear on the RHS of the rule, but which are not used
    ** inside the C code.
    */
    case 227: /* keep */
    case 228: /* tagitemlist */
    case 253: /* fill_opt */
    case 255: /* groupby_opt */
    case 256: /* orderby_opt */
    case 266: /* sortlist */
    case 270: /* grouplist */
{
tVariantListDestroy((yypminor->yy498));
}
      break;
    case 244: /* columnlist */
{
tFieldListDestroy((yypminor->yy523));
}
      break;
    case 245: /* select */
{
doDestroyQuerySql((yypminor->yy414));
}
      break;
    case 248: /* delete */
{
 doDestroyDelSql((yypminor->yy175)); 
}
      break;
    case 250: /* where_opt */
    case 257: /* having_opt */
    case 262: /* expr */
    case 272: /* expritem */
{
tSQLExprDestroy((yypminor->yy64));
}
      break;
    case 251: /* selcollist */
    case 261: /* sclp */
    case 271: /* exprlist */
{
tSQLExprListDestroy((yypminor->yy290));
}
      break;
    case 260: /* union */
{
destroyAllSelectClause((yypminor->yy231));
}
      break;
    case 267: /* sortitem */
{
tVariantDestroy(&(yypminor->yy134));
}
      break;
    default:  break;   /* If no destructor action specified: do nothing */
  }
}

/*
** Pop the parser's stack once.
**
** If there is a destructor routine associated with the token which
** is popped from the stack, then call it.
**
** Return the major token number for the symbol popped.
*/
static int yy_pop_parser_stack(yyParser *pParser){
  YYCODETYPE yymajor;
  yyStackEntry *yytos = &pParser->yystack[pParser->yyidx];

  if( pParser->yyidx<0 ) return 0;
#ifndef NDEBUG
  if( yyTraceFILE && pParser->yyidx>=0 ){
    fprintf(yyTraceFILE,"%sPopping %s\n",
      yyTracePrompt,
      yyTokenName[yytos->major]);
  }
#endif
  yymajor = yytos->major;
  yy_destructor(pParser, yymajor, &yytos->minor);
  pParser->yyidx--;
  return yymajor;
}

/* 
** Deallocate and destroy a parser.  Destructors are all called for
** all stack elements before shutting the parser down.
**
** Inputs:
** <ul>
** <li>  A pointer to the parser.  This should be a pointer
**       obtained from ParseAlloc.
** <li>  A pointer to a function used to reclaim memory obtained
**       from malloc.
** </ul>
*/
void ParseFree(
  void *p,                    /* The parser to be deleted */
  void (*freeProc)(void*)     /* Function used to reclaim memory */
){
  yyParser *pParser = (yyParser*)p;
  if( pParser==0 ) return;
  while( pParser->yyidx>=0 ) yy_pop_parser_stack(pParser);
#if YYSTACKDEPTH<=0
  free(pParser->yystack);
#endif
  (*freeProc)((void*)pParser);
}

/*
** Return the peak depth of the stack for a parser.
*/
#ifdef YYTRACKMAXSTACKDEPTH
int ParseStackPeak(void *p){
  yyParser *pParser = (yyParser*)p;
  return pParser->yyidxMax;
}
#endif

/*
** Find the appropriate action for a parser given the terminal
** look-ahead token iLookAhead.
**
** If the look-ahead token is YYNOCODE, then check to see if the action is
** independent of the look-ahead.  If it is, return the action, otherwise
** return YY_NO_ACTION.
*/
static int yy_find_shift_action(
  yyParser *pParser,        /* The parser */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
  int stateno = pParser->yystack[pParser->yyidx].stateno;
 
  if( stateno>YY_SHIFT_COUNT
   || (i = yy_shift_ofst[stateno])==YY_SHIFT_USE_DFLT ){
    return yy_default[stateno];
  }
  assert( iLookAhead!=YYNOCODE );
  i += iLookAhead;
  if( i<0 || i>=YY_ACTTAB_COUNT || yy_lookahead[i]!=iLookAhead ){
    if( iLookAhead>0 ){
#ifdef YYFALLBACK
      YYCODETYPE iFallback;            /* Fallback token */
      if( iLookAhead<sizeof(yyFallback)/sizeof(yyFallback[0])
             && (iFallback = yyFallback[iLookAhead])!=0 ){
#ifndef NDEBUG
        if( yyTraceFILE ){
          fprintf(yyTraceFILE, "%sFALLBACK %s => %s\n",
             yyTracePrompt, yyTokenName[iLookAhead], yyTokenName[iFallback]);
        }
#endif
        return yy_find_shift_action(pParser, iFallback);
      }
#endif
#ifdef YYWILDCARD
      {
        int j = i - iLookAhead + YYWILDCARD;
        if( 
#if YY_SHIFT_MIN+YYWILDCARD<0
          j>=0 &&
#endif
#if YY_SHIFT_MAX+YYWILDCARD>=YY_ACTTAB_COUNT
          j<YY_ACTTAB_COUNT &&
#endif
          yy_lookahead[j]==YYWILDCARD
        ){
#ifndef NDEBUG
          if( yyTraceFILE ){
            fprintf(yyTraceFILE, "%sWILDCARD %s => %s\n",
               yyTracePrompt, yyTokenName[iLookAhead], yyTokenName[YYWILDCARD]);
          }
#endif /* NDEBUG */
          return yy_action[j];
        }
      }
#endif /* YYWILDCARD */
    }
    return yy_default[stateno];
  }else{
    return yy_action[i];
  }
}

/*
** Find the appropriate action for a parser given the non-terminal
** look-ahead token iLookAhead.
**
** If the look-ahead token is YYNOCODE, then check to see if the action is
** independent of the look-ahead.  If it is, return the action, otherwise
** return YY_NO_ACTION.
*/
static int yy_find_reduce_action(
  int stateno,              /* Current state number */
  YYCODETYPE iLookAhead     /* The look-ahead token */
){
  int i;
#ifdef YYERRORSYMBOL
  if( stateno>YY_REDUCE_COUNT ){
    return yy_default[stateno];
  }
#else
  assert( stateno<=YY_REDUCE_COUNT );
#endif
  i = yy_reduce_ofst[stateno];
  assert( i!=YY_REDUCE_USE_DFLT );
  assert( iLookAhead!=YYNOCODE );
  i += iLookAhead;
#ifdef YYERRORSYMBOL
  if( i<0 || i>=YY_ACTTAB_COUNT || yy_lookahead[i]!=iLookAhead ){
    return yy_default[stateno];
  }
#else
  assert( i>=0 && i<YY_ACTTAB_COUNT );
  assert( yy_lookahead[i]==iLookAhead );
#endif
  return yy_action[i];
}

/*
** The following routine is called if the stack overflows.
*/
static void yyStackOverflow(yyParser *yypParser, YYMINORTYPE *yypMinor){
   ParseARG_FETCH;
   yypParser->yyidx--;
#ifndef NDEBUG
   if( yyTraceFILE ){
     fprintf(yyTraceFILE,"%sStack Overflow!\n",yyTracePrompt);
   }
#endif
   while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
   /* Here code is inserted which will execute if the parser
   ** stack every overflows */
   ParseARG_STORE; /* Suppress warning about unused %extra_argument var */
}

/*
** Perform a shift action.
*/
static void yy_shift(
  yyParser *yypParser,          /* The parser to be shifted */
  int yyNewState,               /* The new state to shift in */
  int yyMajor,                  /* The major token to shift in */
  YYMINORTYPE *yypMinor         /* Pointer to the minor token to shift in */
){
  yyStackEntry *yytos;
  yypParser->yyidx++;
#ifdef YYTRACKMAXSTACKDEPTH
  if( yypParser->yyidx>yypParser->yyidxMax ){
    yypParser->yyidxMax = yypParser->yyidx;
  }
#endif
#if YYSTACKDEPTH>0 
  if( yypParser->yyidx>=YYSTACKDEPTH ){
    yyStackOverflow(yypParser, yypMinor);
    return;
  }
#else
  if( yypParser->yyidx>=yypParser->yystksz ){
    yyGrowStack(yypParser);
    if( yypParser->yyidx>=yypParser->yystksz ){
      yyStackOverflow(yypParser, yypMinor);
      return;
    }
  }
#endif
  yytos = &yypParser->yystack[yypParser->yyidx];
  yytos->stateno = (YYACTIONTYPE)yyNewState;
  yytos->major = (YYCODETYPE)yyMajor;
  yytos->minor = *yypMinor;
#ifndef NDEBUG
  if( yyTraceFILE && yypParser->yyidx>0 ){
    int i;
    fprintf(yyTraceFILE,"%sShift %d\n",yyTracePrompt,yyNewState);
    fprintf(yyTraceFILE,"%sStack:",yyTracePrompt);
    for(i=1; i<=yypParser->yyidx; i++)
      fprintf(yyTraceFILE," %s",yyTokenName[yypParser->yystack[i].major]);
    fprintf(yyTraceFILE,"\n");
  }
#endif
}

/* The following table contains information about every rule that
** is used during the reduce.
*/
static const struct {
  YYCODETYPE lhs;         /* Symbol on the left-hand side of the rule */
  unsigned char nrhs;     /* Number of right-hand side symbols in the rule */
} yyRuleInfo[] = {
  { 208, 1 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 2 },
  { 209, 3 },
  { 210, 0 },
  { 210, 2 },
  { 212, 0 },
  { 212, 2 },
  { 209, 5 },
  { 209, 4 },
  { 209, 3 },
  { 209, 5 },
  { 209, 3 },
  { 209, 5 },
  { 209, 3 },
  { 209, 4 },
  { 209, 5 },
  { 209, 4 },
  { 209, 3 },
  { 209, 3 },
  { 209, 3 },
  { 209, 2 },
  { 209, 3 },
  { 209, 5 },
  { 209, 5 },
  { 209, 4 },
  { 209, 5 },
  { 209, 3 },
  { 209, 4 },
  { 209, 4 },
  { 209, 4 },
  { 209, 6 },
  { 211, 1 },
  { 211, 1 },
  { 213, 2 },
  { 213, 0 },
  { 216, 3 },
  { 216, 0 },
  { 209, 3 },
  { 209, 6 },
  { 209, 5 },
  { 209, 5 },
  { 218, 0 },
  { 218, 2 },
  { 219, 0 },
  { 219, 2 },
  { 220, 0 },
  { 220, 2 },
  { 221, 0 },
  { 221, 2 },
  { 222, 0 },
  { 222, 2 },
  { 223, 0 },
  { 223, 2 },
  { 224, 0 },
  { 224, 2 },
  { 225, 0 },
  { 225, 2 },
  { 226, 0 },
  { 226, 2 },
  { 215, 9 },
  { 227, 2 },
  { 229, 2 },
  { 230, 2 },
  { 231, 2 },
  { 232, 2 },
  { 233, 2 },
  { 234, 2 },
  { 235, 2 },
  { 236, 2 },
  { 237, 2 },
  { 238, 2 },
  { 239, 2 },
  { 240, 2 },
  { 217, 0 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 217, 2 },
  { 214, 0 },
  { 214, 2 },
  { 214, 2 },
  { 214, 2 },
  { 214, 2 },
  { 214, 2 },
  { 214, 2 },
  { 214, 2 },
  { 241, 1 },
  { 241, 4 },
  { 242, 1 },
  { 242, 2 },
  { 242, 2 },
  { 209, 6 },
  { 243, 3 },
  { 243, 7 },
  { 243, 7 },
  { 243, 2 },
  { 244, 3 },
  { 244, 1 },
  { 246, 2 },
  { 228, 3 },
  { 228, 1 },
  { 247, 1 },
  { 247, 1 },
  { 247, 1 },
  { 247, 1 },
  { 247, 1 },
  { 247, 2 },
  { 247, 2 },
  { 247, 2 },
  { 247, 2 },
  { 248, 3 },
  { 209, 1 },
  { 245, 12 },
  { 260, 1 },
  { 260, 3 },
  { 260, 4 },
  { 260, 6 },
  { 209, 1 },
  { 245, 2 },
  { 261, 2 },
  { 261, 0 },
  { 251, 3 },
  { 251, 2 },
  { 263, 2 },
  { 263, 1 },
  { 263, 0 },
  { 249, 2 },
  { 264, 2 },
  { 264, 3 },
  { 264, 4 },
  { 264, 5 },
  { 265, 1 },
  { 252, 4 },
  { 252, 6 },
  { 252, 0 },
  { 253, 0 },
  { 253, 6 },
  { 253, 4 },
  { 254, 4 },
  { 254, 0 },
  { 256, 0 },
  { 256, 3 },
  { 266, 4 },
  { 266, 2 },
  { 268, 2 },
  { 269, 1 },
  { 269, 1 },
  { 269, 0 },
  { 255, 0 },
  { 255, 3 },
  { 270, 3 },
  { 270, 1 },
  { 257, 0 },
  { 257, 2 },
  { 259, 0 },
  { 259, 2 },
  { 259, 4 },
  { 259, 4 },
  { 258, 0 },
  { 258, 2 },
  { 258, 4 },
  { 258, 4 },
  { 250, 0 },
  { 250, 2 },
  { 262, 3 },
  { 262, 1 },
  { 262, 3 },
  { 262, 3 },
  { 262, 1 },
  { 262, 2 },
  { 262, 2 },
  { 262, 1 },
  { 262, 2 },
  { 262, 2 },
  { 262, 1 },
  { 262, 1 },
  { 262, 1 },
  { 262, 1 },
  { 262, 4 },
  { 262, 4 },
  { 262, 3 },
  { 262, 4 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 3 },
  { 262, 5 },
  { 271, 3 },
  { 271, 1 },
  { 272, 1 },
  { 272, 0 },
  { 209, 3 },
  { 209, 7 },
  { 209, 7 },
  { 209, 7 },
  { 209, 7 },
  { 209, 8 },
  { 209, 9 },
  { 209, 3 },
  { 209, 5 },
  { 209, 5 },
};

static void yy_accept(yyParser*);  /* Forward Declaration */

/*
** Perform a reduce action and the shift that must immediately
** follow the reduce.
*/
static void yy_reduce(
  yyParser *yypParser,         /* The parser */
  int yyruleno                 /* Number of the rule by which to reduce */
){
  int yygoto;                     /* The next state */
  int yyact;                      /* The next action */
  YYMINORTYPE yygotominor;        /* The LHS of the rule reduced */
  yyStackEntry *yymsp;            /* The top of the parser's stack */
  int yysize;                     /* Amount to pop the stack */
  ParseARG_FETCH;
  yymsp = &yypParser->yystack[yypParser->yyidx];
#ifndef NDEBUG
  if( yyTraceFILE && yyruleno>=0 
        && yyruleno<(int)(sizeof(yyRuleName)/sizeof(yyRuleName[0])) ){
    fprintf(yyTraceFILE, "%sReduce [%s].\n", yyTracePrompt,
      yyRuleName[yyruleno]);
  }
#endif /* NDEBUG */

  /* Silence complaints from purify about yygotominor being uninitialized
  ** in some cases when it is copied into the stack after the following
  ** switch.  yygotominor is uninitialized when a rule reduces that does
  ** not set the value of its left-hand side nonterminal.  Leaving the
  ** value of the nonterminal uninitialized is utterly harmless as long
  ** as the value is never used.  So really the only thing this code
  ** accomplishes is to quieten purify.  
  **
  ** 2007-01-16:  The wireshark project (www.wireshark.org) reports that
  ** without this code, their parser segfaults.  I'm not sure what there
  ** parser is doing to make this happen.  This is the second bug report
  ** from wireshark this week.  Clearly they are stressing Lemon in ways
  ** that it has not been previously stressed...  (SQLite ticket #2172)
  */
  /*memset(&yygotominor, 0, sizeof(yygotominor));*/
  yygotominor = yyzerominor;


  switch( yyruleno ){
  /* Beginning here are the reduction cases.  A typical example
  ** follows:
  **   case 0:
  **  #line <lineno> <grammarfile>
  **     { ... }           // User supplied code
  **  #line <lineno> <thisfile>
  **     break;
  */
      case 0: /* program ::= cmd */
{}
        break;
      case 1: /* cmd ::= SHOW DATABASES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DB, 0, 0);}
        break;
      case 2: /* cmd ::= SHOW MNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MNODE, 0, 0);}
        break;
      case 3: /* cmd ::= SHOW DNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_DNODE, 0, 0);}
        break;
      case 4: /* cmd ::= SHOW ACCOUNTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_ACCT, 0, 0);}
        break;
      case 5: /* cmd ::= SHOW USERS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_USER, 0, 0);}
        break;
      case 6: /* cmd ::= SHOW MODULES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_MODULE, 0, 0);  }
        break;
      case 7: /* cmd ::= SHOW QUERIES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_QUERIES, 0, 0);  }
        break;
      case 8: /* cmd ::= SHOW CONNECTIONS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_CONNS, 0, 0);}
        break;
      case 9: /* cmd ::= SHOW STREAMS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_STREAMS, 0, 0);  }
        break;
      case 10: /* cmd ::= SHOW VARIABLES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VARIABLES, 0, 0);  }
        break;
      case 11: /* cmd ::= SHOW SCORES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_SCORES, 0, 0);   }
        break;
      case 12: /* cmd ::= SHOW GRANTS */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_GRANTS, 0, 0);   }
        break;
      case 13: /* cmd ::= SHOW VNODES */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, 0, 0); }
        break;
      case 14: /* cmd ::= SHOW VNODES IPTOKEN */
{ setShowOptions(pInfo, TSDB_MGMT_TABLE_VNODES, &yymsp[0].minor.yy0, 0); }
        break;
      case 15: /* dbPrefix ::= */
{yygotominor.yy0.n = 0; yygotominor.yy0.type = 0;}
        break;
      case 16: /* dbPrefix ::= ids DOT */
{yygotominor.yy0 = yymsp[-1].minor.yy0;  }
        break;
      case 17: /* cpxName ::= */
{yygotominor.yy0.n = 0;  }
        break;
      case 18: /* cpxName ::= DOT ids */
{yygotominor.yy0 = yymsp[0].minor.yy0; yygotominor.yy0.n += 1;    }
        break;
      case 19: /* cmd ::= SHOW CREATE TABLE ids cpxName */
{
   yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
   setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 20: /* cmd ::= SHOW CREATE DATABASE ids */
{
  setDCLSQLElems(pInfo, TSDB_SQL_SHOW_CREATE_DATABASE, 1, &yymsp[0].minor.yy0);
}
        break;
      case 21: /* cmd ::= SHOW dbPrefix TABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 22: /* cmd ::= SHOW dbPrefix TABLES LIKE ids */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_TABLE, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0);
}
        break;
      case 23: /* cmd ::= SHOW dbPrefix STABLES */
{
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &yymsp[-1].minor.yy0, 0);
}
        break;
      case 24: /* cmd ::= SHOW dbPrefix STABLES LIKE ids */
{
    SStrToken token;
    setDBName(&token, &yymsp[-3].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_METRIC, &token, &yymsp[0].minor.yy0);
}
        break;
      case 25: /* cmd ::= SHOW dbPrefix VGROUPS */
{
    SStrToken token;
    setDBName(&token, &yymsp[-1].minor.yy0);
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, 0);
}
        break;
      case 26: /* cmd ::= SHOW dbPrefix VGROUPS ids */
{
    SStrToken token;
    setDBName(&token, &yymsp[-2].minor.yy0);    
    setShowOptions(pInfo, TSDB_MGMT_TABLE_VGROUP, &token, &yymsp[0].minor.yy0);
}
        break;
      case 27: /* cmd ::= DROP TABLE ifexists ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDropDBTableInfo(pInfo, TSDB_SQL_DROP_TABLE, &yymsp[-1].minor.yy0, &yymsp[-2].minor.yy0);
}
        break;
      case 28: /* cmd ::= DROP DATABASE ifexists ids */
{ setDropDBTableInfo(pInfo, TSDB_SQL_DROP_DB, &yymsp[0].minor.yy0, &yymsp[-1].minor.yy0); }
        break;
      case 29: /* cmd ::= DROP DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_DNODE, 1, &yymsp[0].minor.yy0);    }
        break;
      case 30: /* cmd ::= DROP USER ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_USER, 1, &yymsp[0].minor.yy0);     }
        break;
      case 31: /* cmd ::= DROP ACCOUNT ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_DROP_ACCT, 1, &yymsp[0].minor.yy0);  }
        break;
      case 32: /* cmd ::= USE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_USE_DB, 1, &yymsp[0].minor.yy0);}
        break;
      case 33: /* cmd ::= DESCRIBE ids cpxName */
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    setDCLSQLElems(pInfo, TSDB_SQL_DESCRIBE_TABLE, 1, &yymsp[-1].minor.yy0);
}
        break;
      case 34: /* cmd ::= ALTER USER ids PASS ids */
{ setAlterUserSQL(pInfo, TSDB_ALTER_USER_PASSWD, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0, NULL);    }
        break;
      case 35: /* cmd ::= ALTER USER ids PRIVILEGE ids */
{ setAlterUserSQL(pInfo, TSDB_ALTER_USER_PRIVILEGES, &yymsp[-2].minor.yy0, NULL, &yymsp[0].minor.yy0);}
        break;
      case 36: /* cmd ::= ALTER DNODE ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 37: /* cmd ::= ALTER DNODE ids ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_DNODE, 3, &yymsp[-2].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);      }
        break;
      case 38: /* cmd ::= ALTER LOCAL ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 1, &yymsp[0].minor.yy0);              }
        break;
      case 39: /* cmd ::= ALTER LOCAL ids ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CFG_LOCAL, 2, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy0);          }
        break;
      case 40: /* cmd ::= ALTER DATABASE ids alter_db_optr */
{ SStrToken t = {0};  setCreateDBSQL(pInfo, TSDB_SQL_ALTER_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy268, &t);}
        break;
      case 41: /* cmd ::= ALTER ACCOUNT ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-1].minor.yy0, NULL, &yymsp[0].minor.yy149);}
        break;
      case 42: /* cmd ::= ALTER ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_ALTER_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy149);}
        break;
      case 43: /* ids ::= ID */
      case 44: /* ids ::= STRING */ yytestcase(yyruleno==44);
{yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 45: /* ifexists ::= IF EXISTS */
      case 47: /* ifnotexists ::= IF NOT EXISTS */ yytestcase(yyruleno==47);
{yygotominor.yy0.n = 1;}
        break;
      case 46: /* ifexists ::= */
      case 48: /* ifnotexists ::= */ yytestcase(yyruleno==48);
{yygotominor.yy0.n = 0;}
        break;
      case 49: /* cmd ::= CREATE DNODE ids */
{ setDCLSQLElems(pInfo, TSDB_SQL_CREATE_DNODE, 1, &yymsp[0].minor.yy0);}
        break;
      case 50: /* cmd ::= CREATE ACCOUNT ids PASS ids acct_optr */
{ setCreateAcctSQL(pInfo, TSDB_SQL_CREATE_ACCT, &yymsp[-3].minor.yy0, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy149);}
        break;
      case 51: /* cmd ::= CREATE DATABASE ifnotexists ids db_optr */
{ setCreateDBSQL(pInfo, TSDB_SQL_CREATE_DB, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy268, &yymsp[-2].minor.yy0);}
        break;
      case 52: /* cmd ::= CREATE USER ids PASS ids */
{ setCreateUserSQL(pInfo, &yymsp[-2].minor.yy0, &yymsp[0].minor.yy0);}
        break;
      case 53: /* pps ::= */
      case 55: /* tseries ::= */ yytestcase(yyruleno==55);
      case 57: /* dbs ::= */ yytestcase(yyruleno==57);
      case 59: /* streams ::= */ yytestcase(yyruleno==59);
      case 61: /* storage ::= */ yytestcase(yyruleno==61);
      case 63: /* qtime ::= */ yytestcase(yyruleno==63);
      case 65: /* users ::= */ yytestcase(yyruleno==65);
      case 67: /* conns ::= */ yytestcase(yyruleno==67);
      case 69: /* state ::= */ yytestcase(yyruleno==69);
{yygotominor.yy0.n = 0;   }
        break;
      case 54: /* pps ::= PPS INTEGER */
      case 56: /* tseries ::= TSERIES INTEGER */ yytestcase(yyruleno==56);
      case 58: /* dbs ::= DBS INTEGER */ yytestcase(yyruleno==58);
      case 60: /* streams ::= STREAMS INTEGER */ yytestcase(yyruleno==60);
      case 62: /* storage ::= STORAGE INTEGER */ yytestcase(yyruleno==62);
      case 64: /* qtime ::= QTIME INTEGER */ yytestcase(yyruleno==64);
      case 66: /* users ::= USERS INTEGER */ yytestcase(yyruleno==66);
      case 68: /* conns ::= CONNS INTEGER */ yytestcase(yyruleno==68);
      case 70: /* state ::= STATE ids */ yytestcase(yyruleno==70);
{yygotominor.yy0 = yymsp[0].minor.yy0;     }
        break;
      case 71: /* acct_optr ::= pps tseries storage streams qtime dbs users conns state */
{
    yygotominor.yy149.maxUsers   = (yymsp[-2].minor.yy0.n>0)?atoi(yymsp[-2].minor.yy0.z):-1;
    yygotominor.yy149.maxDbs     = (yymsp[-3].minor.yy0.n>0)?atoi(yymsp[-3].minor.yy0.z):-1;
    yygotominor.yy149.maxTimeSeries = (yymsp[-7].minor.yy0.n>0)?atoi(yymsp[-7].minor.yy0.z):-1;
    yygotominor.yy149.maxStreams = (yymsp[-5].minor.yy0.n>0)?atoi(yymsp[-5].minor.yy0.z):-1;
    yygotominor.yy149.maxPointsPerSecond     = (yymsp[-8].minor.yy0.n>0)?atoi(yymsp[-8].minor.yy0.z):-1;
    yygotominor.yy149.maxStorage = (yymsp[-6].minor.yy0.n>0)?strtoll(yymsp[-6].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy149.maxQueryTime   = (yymsp[-4].minor.yy0.n>0)?strtoll(yymsp[-4].minor.yy0.z, NULL, 10):-1;
    yygotominor.yy149.maxConnections   = (yymsp[-1].minor.yy0.n>0)?atoi(yymsp[-1].minor.yy0.z):-1;
    yygotominor.yy149.stat    = yymsp[0].minor.yy0;
}
        break;
      case 72: /* keep ::= KEEP tagitemlist */
{ yygotominor.yy498 = yymsp[0].minor.yy498; }
        break;
      case 73: /* cache ::= CACHE INTEGER */
      case 74: /* replica ::= REPLICA INTEGER */ yytestcase(yyruleno==74);
      case 75: /* quorum ::= QUORUM INTEGER */ yytestcase(yyruleno==75);
      case 76: /* days ::= DAYS INTEGER */ yytestcase(yyruleno==76);
      case 77: /* minrows ::= MINROWS INTEGER */ yytestcase(yyruleno==77);
      case 78: /* maxrows ::= MAXROWS INTEGER */ yytestcase(yyruleno==78);
      case 79: /* blocks ::= BLOCKS INTEGER */ yytestcase(yyruleno==79);
      case 80: /* ctime ::= CTIME INTEGER */ yytestcase(yyruleno==80);
      case 81: /* wal ::= WAL INTEGER */ yytestcase(yyruleno==81);
      case 82: /* fsync ::= FSYNC INTEGER */ yytestcase(yyruleno==82);
      case 83: /* comp ::= COMP INTEGER */ yytestcase(yyruleno==83);
      case 84: /* prec ::= PRECISION STRING */ yytestcase(yyruleno==84);
{ yygotominor.yy0 = yymsp[0].minor.yy0; }
        break;
      case 85: /* db_optr ::= */
{setDefaultCreateDbOption(&yygotominor.yy268);}
        break;
      case 86: /* db_optr ::= db_optr cache */
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.cacheBlockSize = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 87: /* db_optr ::= db_optr replica */
      case 100: /* alter_db_optr ::= alter_db_optr replica */ yytestcase(yyruleno==100);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.replica = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 88: /* db_optr ::= db_optr quorum */
      case 101: /* alter_db_optr ::= alter_db_optr quorum */ yytestcase(yyruleno==101);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.quorum = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 89: /* db_optr ::= db_optr days */
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.daysPerFile = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 90: /* db_optr ::= db_optr minrows */
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.minRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 91: /* db_optr ::= db_optr maxrows */
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.maxRowsPerBlock = strtod(yymsp[0].minor.yy0.z, NULL); }
        break;
      case 92: /* db_optr ::= db_optr blocks */
      case 103: /* alter_db_optr ::= alter_db_optr blocks */ yytestcase(yyruleno==103);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.numOfBlocks = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 93: /* db_optr ::= db_optr ctime */
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.commitTime = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 94: /* db_optr ::= db_optr wal */
      case 105: /* alter_db_optr ::= alter_db_optr wal */ yytestcase(yyruleno==105);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.walLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 95: /* db_optr ::= db_optr fsync */
      case 106: /* alter_db_optr ::= alter_db_optr fsync */ yytestcase(yyruleno==106);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.fsyncPeriod = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 96: /* db_optr ::= db_optr comp */
      case 104: /* alter_db_optr ::= alter_db_optr comp */ yytestcase(yyruleno==104);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.compressionLevel = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 97: /* db_optr ::= db_optr prec */
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.precision = yymsp[0].minor.yy0; }
        break;
      case 98: /* db_optr ::= db_optr keep */
      case 102: /* alter_db_optr ::= alter_db_optr keep */ yytestcase(yyruleno==102);
{ yygotominor.yy268 = yymsp[-1].minor.yy268; yygotominor.yy268.keep = yymsp[0].minor.yy498; }
        break;
      case 99: /* alter_db_optr ::= */
{ setDefaultCreateDbOption(&yygotominor.yy268);}
        break;
      case 107: /* typename ::= ids */
{ 
  yymsp[0].minor.yy0.type = 0;
  tSQLSetColumnType (&yygotominor.yy223, &yymsp[0].minor.yy0); 
}
        break;
      case 108: /* typename ::= ids LP signed RP */
{
    if (yymsp[-1].minor.yy207 <= 0) {
      yymsp[-3].minor.yy0.type = 0;
      tSQLSetColumnType(&yygotominor.yy223, &yymsp[-3].minor.yy0);
    } else {
      yymsp[-3].minor.yy0.type = -yymsp[-1].minor.yy207;          // negative value of name length
      tSQLSetColumnType(&yygotominor.yy223, &yymsp[-3].minor.yy0);
    }
}
        break;
      case 109: /* signed ::= INTEGER */
      case 110: /* signed ::= PLUS INTEGER */ yytestcase(yyruleno==110);
{ yygotominor.yy207 = strtol(yymsp[0].minor.yy0.z, NULL, 10); }
        break;
      case 111: /* signed ::= MINUS INTEGER */
{ yygotominor.yy207 = -strtol(yymsp[0].minor.yy0.z, NULL, 10);}
        break;
      case 112: /* cmd ::= CREATE TABLE ifnotexists ids cpxName create_table_args */
{
    yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
    setCreatedTableName(pInfo, &yymsp[-2].minor.yy0, &yymsp[-3].minor.yy0);
}
        break;
      case 113: /* create_table_args ::= LP columnlist RP */
{
    yygotominor.yy470 = tSetCreateSQLElems(yymsp[-1].minor.yy523, NULL, NULL, NULL, NULL, TSQL_CREATE_TABLE);
    setSQLInfo(pInfo, yygotominor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 114: /* create_table_args ::= LP columnlist RP TAGS LP columnlist RP */
{
    yygotominor.yy470 = tSetCreateSQLElems(yymsp[-5].minor.yy523, yymsp[-1].minor.yy523, NULL, NULL, NULL, TSQL_CREATE_STABLE);
    setSQLInfo(pInfo, yygotominor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 115: /* create_table_args ::= USING ids cpxName TAGS LP tagitemlist RP */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;
    yygotominor.yy470 = tSetCreateSQLElems(NULL, NULL, &yymsp[-5].minor.yy0, yymsp[-1].minor.yy498, NULL, TSQL_CREATE_TABLE_FROM_STABLE);
    setSQLInfo(pInfo, yygotominor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 116: /* create_table_args ::= AS select */
{
    yygotominor.yy470 = tSetCreateSQLElems(NULL, NULL, NULL, NULL, yymsp[0].minor.yy414, TSQL_CREATE_STREAM);
    setSQLInfo(pInfo, yygotominor.yy470, NULL, TSDB_SQL_CREATE_TABLE);
}
        break;
      case 117: /* columnlist ::= columnlist COMMA column */
{yygotominor.yy523 = tFieldListAppend(yymsp[-2].minor.yy523, &yymsp[0].minor.yy223);   }
        break;
      case 118: /* columnlist ::= column */
{yygotominor.yy523 = tFieldListAppend(NULL, &yymsp[0].minor.yy223);}
        break;
      case 119: /* column ::= ids typename */
{
    tSQLSetColumnInfo(&yygotominor.yy223, &yymsp[-1].minor.yy0, &yymsp[0].minor.yy223);
}
        break;
      case 120: /* tagitemlist ::= tagitemlist COMMA tagitem */
{ yygotominor.yy498 = tVariantListAppend(yymsp[-2].minor.yy498, &yymsp[0].minor.yy134, -1);    }
        break;
      case 121: /* tagitemlist ::= tagitem */
{ yygotominor.yy498 = tVariantListAppend(NULL, &yymsp[0].minor.yy134, -1); }
        break;
      case 122: /* tagitem ::= INTEGER */
      case 123: /* tagitem ::= FLOAT */ yytestcase(yyruleno==123);
      case 124: /* tagitem ::= STRING */ yytestcase(yyruleno==124);
      case 125: /* tagitem ::= BOOL */ yytestcase(yyruleno==125);
{toTSDBType(yymsp[0].minor.yy0.type); tVariantCreate(&yygotominor.yy134, &yymsp[0].minor.yy0); }
        break;
      case 126: /* tagitem ::= NULL */
{ yymsp[0].minor.yy0.type = 0; tVariantCreate(&yygotominor.yy134, &yymsp[0].minor.yy0); }
        break;
      case 127: /* tagitem ::= MINUS INTEGER */
      case 128: /* tagitem ::= MINUS FLOAT */ yytestcase(yyruleno==128);
      case 129: /* tagitem ::= PLUS INTEGER */ yytestcase(yyruleno==129);
      case 130: /* tagitem ::= PLUS FLOAT */ yytestcase(yyruleno==130);
{
    yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
    yymsp[-1].minor.yy0.type = yymsp[0].minor.yy0.type;
    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantCreate(&yygotominor.yy134, &yymsp[-1].minor.yy0);
}
        break;
      case 131: /* delete ::= DELETE from where_opt */
{
  yygotominor.yy175 = tSetDelSQLElems(yymsp[-1].minor.yy498, yymsp[0].minor.yy64);
}
        break;
      case 132: /* cmd ::= delete */
{ setSQLInfo(pInfo, yymsp[0].minor.yy175, NULL, TSDB_SQL_DELETE);  }
        break;
      case 133: /* select ::= SELECT selcollist from where_opt interval_opt fill_opt sliding_opt groupby_opt orderby_opt having_opt slimit_opt limit_opt */
{
  yygotominor.yy414 = tSetQuerySQLElems(&yymsp[-11].minor.yy0, yymsp[-10].minor.yy290, yymsp[-9].minor.yy498, yymsp[-8].minor.yy64, yymsp[-4].minor.yy498, yymsp[-3].minor.yy498, &yymsp[-7].minor.yy532, &yymsp[-5].minor.yy0, yymsp[-6].minor.yy498, &yymsp[0].minor.yy216, &yymsp[-1].minor.yy216);
}
        break;
      case 134: /* union ::= select */
{ yygotominor.yy231 = setSubclause(NULL, yymsp[0].minor.yy414); }
        break;
      case 135: /* union ::= LP union RP */
{ yygotominor.yy231 = yymsp[-1].minor.yy231; }
        break;
      case 136: /* union ::= union UNION ALL select */
{ yygotominor.yy231 = appendSelectClause(yymsp[-3].minor.yy231, yymsp[0].minor.yy414); }
        break;
      case 137: /* union ::= union UNION ALL LP select RP */
{ yygotominor.yy231 = appendSelectClause(yymsp[-5].minor.yy231, yymsp[-1].minor.yy414); }
        break;
      case 138: /* cmd ::= union */
{ setSQLInfo(pInfo, yymsp[0].minor.yy231, NULL, TSDB_SQL_SELECT); }
        break;
      case 139: /* select ::= SELECT selcollist */
{
  yygotominor.yy414 = tSetQuerySQLElems(&yymsp[-1].minor.yy0, yymsp[0].minor.yy290, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
}
        break;
      case 140: /* sclp ::= selcollist COMMA */
{yygotominor.yy290 = yymsp[-1].minor.yy290;}
        break;
      case 141: /* sclp ::= */
{yygotominor.yy290 = 0;}
        break;
      case 142: /* selcollist ::= sclp expr as */
{
   yygotominor.yy290 = tSQLExprListAppend(yymsp[-2].minor.yy290, yymsp[-1].minor.yy64, yymsp[0].minor.yy0.n?&yymsp[0].minor.yy0:0);
}
        break;
      case 143: /* selcollist ::= sclp STAR */
{
   tSQLExpr *pNode = tSQLExprIdValueCreate(NULL, TK_ALL);
   yygotominor.yy290 = tSQLExprListAppend(yymsp[-1].minor.yy290, pNode, 0);
}
        break;
      case 144: /* as ::= AS ids */
      case 145: /* as ::= ids */ yytestcase(yyruleno==145);
{ yygotominor.yy0 = yymsp[0].minor.yy0;    }
        break;
      case 146: /* as ::= */
{ yygotominor.yy0.n = 0;  }
        break;
      case 147: /* from ::= FROM tablelist */
      case 162: /* orderby_opt ::= ORDER BY sortlist */ yytestcase(yyruleno==162);
      case 170: /* groupby_opt ::= GROUP BY grouplist */ yytestcase(yyruleno==170);
{yygotominor.yy498 = yymsp[0].minor.yy498;}
        break;
      case 148: /* tablelist ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy498 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
  yygotominor.yy498 = tVariantListAppendToken(yygotominor.yy498, &yymsp[-1].minor.yy0, -1);  // table alias name
}
        break;
      case 149: /* tablelist ::= ids cpxName ids */
{
   toTSDBType(yymsp[-2].minor.yy0.type);
   toTSDBType(yymsp[0].minor.yy0.type);
   yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
   yygotominor.yy498 = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
   yygotominor.yy498 = tVariantListAppendToken(yygotominor.yy498, &yymsp[0].minor.yy0, -1);
}
        break;
      case 150: /* tablelist ::= tablelist COMMA ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;
  yygotominor.yy498 = tVariantListAppendToken(yymsp[-3].minor.yy498, &yymsp[-1].minor.yy0, -1);
  yygotominor.yy498 = tVariantListAppendToken(yygotominor.yy498, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 151: /* tablelist ::= tablelist COMMA ids cpxName ids */
{
   toTSDBType(yymsp[-2].minor.yy0.type);
   toTSDBType(yymsp[0].minor.yy0.type);
   yymsp[-2].minor.yy0.n += yymsp[-1].minor.yy0.n;
   yygotominor.yy498 = tVariantListAppendToken(yymsp[-4].minor.yy498, &yymsp[-2].minor.yy0, -1);
   yygotominor.yy498 = tVariantListAppendToken(yygotominor.yy498, &yymsp[0].minor.yy0, -1);
}
        break;
      case 152: /* tmvar ::= VARIABLE */
{yygotominor.yy0 = yymsp[0].minor.yy0;}
        break;
      case 153: /* interval_opt ::= INTERVAL LP tmvar RP */
{yygotominor.yy532.interval = yymsp[-1].minor.yy0; yygotominor.yy532.offset.n = 0; yygotominor.yy532.offset.z = NULL; yygotominor.yy532.offset.type = 0;}
        break;
      case 154: /* interval_opt ::= INTERVAL LP tmvar COMMA tmvar RP */
{yygotominor.yy532.interval = yymsp[-3].minor.yy0; yygotominor.yy532.offset = yymsp[-1].minor.yy0;}
        break;
      case 155: /* interval_opt ::= */
{memset(&yygotominor.yy532, 0, sizeof(yygotominor.yy532));}
        break;
      case 156: /* fill_opt ::= */
{yygotominor.yy498 = 0;     }
        break;
      case 157: /* fill_opt ::= FILL LP ID COMMA tagitemlist RP */
{
    tVariant A = {0};
    toTSDBType(yymsp[-3].minor.yy0.type);
    tVariantCreate(&A, &yymsp[-3].minor.yy0);

    tVariantListInsert(yymsp[-1].minor.yy498, &A, -1, 0);
    yygotominor.yy498 = yymsp[-1].minor.yy498;
}
        break;
      case 158: /* fill_opt ::= FILL LP ID RP */
{
    toTSDBType(yymsp[-1].minor.yy0.type);
    yygotominor.yy498 = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);
}
        break;
      case 159: /* sliding_opt ::= SLIDING LP tmvar RP */
{yygotominor.yy0 = yymsp[-1].minor.yy0;     }
        break;
      case 160: /* sliding_opt ::= */
{yygotominor.yy0.n = 0; yygotominor.yy0.z = NULL; yygotominor.yy0.type = 0;   }
        break;
      case 161: /* orderby_opt ::= */
      case 169: /* groupby_opt ::= */ yytestcase(yyruleno==169);
{yygotominor.yy498 = 0;}
        break;
      case 163: /* sortlist ::= sortlist COMMA item sortorder */
{
    yygotominor.yy498 = tVariantListAppend(yymsp[-3].minor.yy498, &yymsp[-1].minor.yy134, yymsp[0].minor.yy46);
}
        break;
      case 164: /* sortlist ::= item sortorder */
{
  yygotominor.yy498 = tVariantListAppend(NULL, &yymsp[-1].minor.yy134, yymsp[0].minor.yy46);
}
        break;
      case 165: /* item ::= ids cpxName */
{
  toTSDBType(yymsp[-1].minor.yy0.type);
  yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n;

  tVariantCreate(&yygotominor.yy134, &yymsp[-1].minor.yy0);
}
        break;
      case 166: /* sortorder ::= ASC */
{yygotominor.yy46 = TSDB_ORDER_ASC; }
        break;
      case 167: /* sortorder ::= DESC */
{yygotominor.yy46 = TSDB_ORDER_DESC;}
        break;
      case 168: /* sortorder ::= */
{yygotominor.yy46 = TSDB_ORDER_ASC;}
        break;
      case 171: /* grouplist ::= grouplist COMMA item */
{
  yygotominor.yy498 = tVariantListAppend(yymsp[-2].minor.yy498, &yymsp[0].minor.yy134, -1);
}
        break;
      case 172: /* grouplist ::= item */
{
  yygotominor.yy498 = tVariantListAppend(NULL, &yymsp[0].minor.yy134, -1);
}
        break;
      case 173: /* having_opt ::= */
      case 183: /* where_opt ::= */ yytestcase(yyruleno==183);
      case 221: /* expritem ::= */ yytestcase(yyruleno==221);
{yygotominor.yy64 = 0;}
        break;
      case 174: /* having_opt ::= HAVING expr */
      case 184: /* where_opt ::= WHERE expr */ yytestcase(yyruleno==184);
      case 220: /* expritem ::= expr */ yytestcase(yyruleno==220);
{yygotominor.yy64 = yymsp[0].minor.yy64;}
        break;
      case 175: /* limit_opt ::= */
      case 179: /* slimit_opt ::= */ yytestcase(yyruleno==179);
{yygotominor.yy216.limit = -1; yygotominor.yy216.offset = 0;}
        break;
      case 176: /* limit_opt ::= LIMIT signed */
      case 180: /* slimit_opt ::= SLIMIT signed */ yytestcase(yyruleno==180);
{yygotominor.yy216.limit = yymsp[0].minor.yy207;  yygotominor.yy216.offset = 0;}
        break;
      case 177: /* limit_opt ::= LIMIT signed OFFSET signed */
      case 181: /* slimit_opt ::= SLIMIT signed SOFFSET signed */ yytestcase(yyruleno==181);
{yygotominor.yy216.limit = yymsp[-2].minor.yy207;  yygotominor.yy216.offset = yymsp[0].minor.yy207;}
        break;
      case 178: /* limit_opt ::= LIMIT signed COMMA signed */
      case 182: /* slimit_opt ::= SLIMIT signed COMMA signed */ yytestcase(yyruleno==182);
{yygotominor.yy216.limit = yymsp[0].minor.yy207;  yygotominor.yy216.offset = yymsp[-2].minor.yy207;}
        break;
      case 185: /* expr ::= LP expr RP */
{yygotominor.yy64 = yymsp[-1].minor.yy64; }
        break;
      case 186: /* expr ::= ID */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_ID);}
        break;
      case 187: /* expr ::= ID DOT ID */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ID);}
        break;
      case 188: /* expr ::= ID DOT STAR */
{yymsp[-2].minor.yy0.n += (1+yymsp[0].minor.yy0.n); yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[-2].minor.yy0, TK_ALL);}
        break;
      case 189: /* expr ::= INTEGER */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_INTEGER);}
        break;
      case 190: /* expr ::= MINUS INTEGER */
      case 191: /* expr ::= PLUS INTEGER */ yytestcase(yyruleno==191);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_INTEGER; yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_INTEGER);}
        break;
      case 192: /* expr ::= FLOAT */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_FLOAT);}
        break;
      case 193: /* expr ::= MINUS FLOAT */
      case 194: /* expr ::= PLUS FLOAT */ yytestcase(yyruleno==194);
{yymsp[-1].minor.yy0.n += yymsp[0].minor.yy0.n; yymsp[-1].minor.yy0.type = TK_FLOAT; yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[-1].minor.yy0, TK_FLOAT);}
        break;
      case 195: /* expr ::= STRING */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_STRING);}
        break;
      case 196: /* expr ::= NOW */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_NOW); }
        break;
      case 197: /* expr ::= VARIABLE */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_VARIABLE);}
        break;
      case 198: /* expr ::= BOOL */
{yygotominor.yy64 = tSQLExprIdValueCreate(&yymsp[0].minor.yy0, TK_BOOL);}
        break;
      case 199: /* expr ::= ID LP exprlist RP */
{ yygotominor.yy64 = tSQLExprCreateFunction(yymsp[-1].minor.yy290, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 200: /* expr ::= ID LP STAR RP */
{ yygotominor.yy64 = tSQLExprCreateFunction(NULL, &yymsp[-3].minor.yy0, &yymsp[0].minor.yy0, yymsp[-3].minor.yy0.type); }
        break;
      case 201: /* expr ::= expr IS NULL */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, NULL, TK_ISNULL);}
        break;
      case 202: /* expr ::= expr IS NOT NULL */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-3].minor.yy64, NULL, TK_NOTNULL);}
        break;
      case 203: /* expr ::= expr LT expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_LT);}
        break;
      case 204: /* expr ::= expr GT expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_GT);}
        break;
      case 205: /* expr ::= expr LE expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_LE);}
        break;
      case 206: /* expr ::= expr GE expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_GE);}
        break;
      case 207: /* expr ::= expr NE expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_NE);}
        break;
      case 208: /* expr ::= expr EQ expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_EQ);}
        break;
      case 209: /* expr ::= expr AND expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_AND);}
        break;
      case 210: /* expr ::= expr OR expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_OR); }
        break;
      case 211: /* expr ::= expr PLUS expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_PLUS);  }
        break;
      case 212: /* expr ::= expr MINUS expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_MINUS); }
        break;
      case 213: /* expr ::= expr STAR expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_STAR);  }
        break;
      case 214: /* expr ::= expr SLASH expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_DIVIDE);}
        break;
      case 215: /* expr ::= expr REM expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_REM);   }
        break;
      case 216: /* expr ::= expr LIKE expr */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-2].minor.yy64, yymsp[0].minor.yy64, TK_LIKE);  }
        break;
      case 217: /* expr ::= expr IN LP exprlist RP */
{yygotominor.yy64 = tSQLExprCreate(yymsp[-4].minor.yy64, (tSQLExpr*)yymsp[-1].minor.yy290, TK_IN); }
        break;
      case 218: /* exprlist ::= exprlist COMMA expritem */
{yygotominor.yy290 = tSQLExprListAppend(yymsp[-2].minor.yy290,yymsp[0].minor.yy64,0);}
        break;
      case 219: /* exprlist ::= expritem */
{yygotominor.yy290 = tSQLExprListAppend(0,yymsp[0].minor.yy64,0);}
        break;
      case 222: /* cmd ::= RESET QUERY CACHE */
{ setDCLSQLElems(pInfo, TSDB_SQL_RESET_CACHE, 0);}
        break;
      case 223: /* cmd ::= ALTER TABLE ids cpxName ADD COLUMN columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy523, NULL, TSDB_ALTER_TABLE_ADD_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 224: /* cmd ::= ALTER TABLE ids cpxName DROP COLUMN ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* K = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, K, TSDB_ALTER_TABLE_DROP_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 225: /* cmd ::= ALTER TABLE ids cpxName ADD TAG columnlist */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;
    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, yymsp[0].minor.yy523, NULL, TSDB_ALTER_TABLE_ADD_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 226: /* cmd ::= ALTER TABLE ids cpxName DROP TAG ids */
{
    yymsp[-4].minor.yy0.n += yymsp[-3].minor.yy0.n;

    toTSDBType(yymsp[0].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-4].minor.yy0, NULL, A, TSDB_ALTER_TABLE_DROP_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 227: /* cmd ::= ALTER TABLE ids cpxName CHANGE TAG ids ids */
{
    yymsp[-5].minor.yy0.n += yymsp[-4].minor.yy0.n;

    toTSDBType(yymsp[-1].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-1].minor.yy0, -1);

    toTSDBType(yymsp[0].minor.yy0.type);
    A = tVariantListAppendToken(A, &yymsp[0].minor.yy0, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-5].minor.yy0, NULL, A, TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 228: /* cmd ::= ALTER TABLE ids cpxName SET TAG ids EQ tagitem */
{
    yymsp[-6].minor.yy0.n += yymsp[-5].minor.yy0.n;

    toTSDBType(yymsp[-2].minor.yy0.type);
    tVariantList* A = tVariantListAppendToken(NULL, &yymsp[-2].minor.yy0, -1);
    A = tVariantListAppend(A, &yymsp[0].minor.yy134, -1);

    SAlterTableSQL* pAlterTable = tAlterTableSQLElems(&yymsp[-6].minor.yy0, NULL, A, TSDB_ALTER_TABLE_UPDATE_TAG_VAL);
    setSQLInfo(pInfo, pAlterTable, NULL, TSDB_SQL_ALTER_TABLE);
}
        break;
      case 229: /* cmd ::= KILL CONNECTION INTEGER */
{setKillSQL(pInfo, TSDB_SQL_KILL_CONNECTION, &yymsp[0].minor.yy0);}
        break;
      case 230: /* cmd ::= KILL STREAM INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_STREAM, &yymsp[-2].minor.yy0);}
        break;
      case 231: /* cmd ::= KILL QUERY INTEGER COLON INTEGER */
{yymsp[-2].minor.yy0.n += (yymsp[-1].minor.yy0.n + yymsp[0].minor.yy0.n); setKillSQL(pInfo, TSDB_SQL_KILL_QUERY, &yymsp[-2].minor.yy0);}
        break;
      default:
        break;
  };
  yygoto = yyRuleInfo[yyruleno].lhs;
  yysize = yyRuleInfo[yyruleno].nrhs;
  yypParser->yyidx -= yysize;
  yyact = yy_find_reduce_action(yymsp[-yysize].stateno,(YYCODETYPE)yygoto);
  if( yyact < YYNSTATE ){
#ifdef NDEBUG
    /* If we are not debugging and the reduce action popped at least
    ** one element off the stack, then we can push the new element back
    ** onto the stack here, and skip the stack overflow test in yy_shift().
    ** That gives a significant speed improvement. */
    if( yysize ){
      yypParser->yyidx++;
      yymsp -= yysize-1;
      yymsp->stateno = (YYACTIONTYPE)yyact;
      yymsp->major = (YYCODETYPE)yygoto;
      yymsp->minor = yygotominor;
    }else
#endif
    {
      yy_shift(yypParser,yyact,yygoto,&yygotominor);
    }
  }else{
    assert( yyact == YYNSTATE + YYNRULE + 1 );
    yy_accept(yypParser);
  }
}

/*
** The following code executes when the parse fails
*/
#ifndef YYNOERRORRECOVERY
static void yy_parse_failed(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH;
#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sFail!\n",yyTracePrompt);
  }
#endif
  while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
  /* Here code is inserted which will be executed whenever the
  ** parser fails */
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
}
#endif /* YYNOERRORRECOVERY */

/*
** The following code executes when a syntax error first occurs.
*/
static void yy_syntax_error(
  yyParser *yypParser,           /* The parser */
  int yymajor,                   /* The major type of the error token */
  YYMINORTYPE yyminor            /* The minor type of the error token */
){
  ParseARG_FETCH;
#define TOKEN (yyminor.yy0)

  pInfo->valid = false;
  int32_t outputBufLen = tListLen(pInfo->pzErrMsg);
  int32_t len = 0;

  if(TOKEN.z) {
    char msg[] = "syntax error near \"%s\"";
    int32_t sqlLen = strlen(&TOKEN.z[0]);

    if (sqlLen + sizeof(msg)/sizeof(msg[0]) + 1 > outputBufLen) {
        char tmpstr[128] = {0};
        memcpy(tmpstr, &TOKEN.z[0], sizeof(tmpstr)/sizeof(tmpstr[0]) - 1);
        len = sprintf(pInfo->pzErrMsg, msg, tmpstr);
    } else {
        len = sprintf(pInfo->pzErrMsg, msg, &TOKEN.z[0]);
    }

  } else {
    len = sprintf(pInfo->pzErrMsg, "Incomplete SQL statement");
  }

  assert(len <= outputBufLen);
  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
}

/*
** The following is executed when the parser accepts
*/
static void yy_accept(
  yyParser *yypParser           /* The parser */
){
  ParseARG_FETCH;
#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sAccept!\n",yyTracePrompt);
  }
#endif
  while( yypParser->yyidx>=0 ) yy_pop_parser_stack(yypParser);
  /* Here code is inserted which will be executed whenever the
  ** parser accepts */

  ParseARG_STORE; /* Suppress warning about unused %extra_argument variable */
}

/* The main parser program.
** The first argument is a pointer to a structure obtained from
** "ParseAlloc" which describes the current state of the parser.
** The second argument is the major token number.  The third is
** the minor token.  The fourth optional argument is whatever the
** user wants (and specified in the grammar) and is available for
** use by the action routines.
**
** Inputs:
** <ul>
** <li> A pointer to the parser (an opaque structure.)
** <li> The major token number.
** <li> The minor token number.
** <li> An option argument of a grammar-specified type.
** </ul>
**
** Outputs:
** None.
*/
void Parse(
  void *yyp,                   /* The parser */
  int yymajor,                 /* The major token code number */
  ParseTOKENTYPE yyminor       /* The value for the token */
  ParseARG_PDECL               /* Optional %extra_argument parameter */
){
  YYMINORTYPE yyminorunion;
  int yyact;            /* The parser action. */
  int yyendofinput;     /* True if we are at the end of input */
#ifdef YYERRORSYMBOL
  int yyerrorhit = 0;   /* True if yymajor has invoked an error */
#endif
  yyParser *yypParser;  /* The parser */

  /* (re)initialize the parser, if necessary */
  yypParser = (yyParser*)yyp;
  if( yypParser->yyidx<0 ){
#if YYSTACKDEPTH<=0
    if( yypParser->yystksz <=0 ){
      /*memset(&yyminorunion, 0, sizeof(yyminorunion));*/
      yyminorunion = yyzerominor;
      yyStackOverflow(yypParser, &yyminorunion);
      return;
    }
#endif
    yypParser->yyidx = 0;
    yypParser->yyerrcnt = -1;
    yypParser->yystack[0].stateno = 0;
    yypParser->yystack[0].major = 0;
  }
  yyminorunion.yy0 = yyminor;
  yyendofinput = (yymajor==0);
  ParseARG_STORE;

#ifndef NDEBUG
  if( yyTraceFILE ){
    fprintf(yyTraceFILE,"%sInput %s\n",yyTracePrompt,yyTokenName[yymajor]);
  }
#endif

  do{
    yyact = yy_find_shift_action(yypParser,(YYCODETYPE)yymajor);
    if( yyact<YYNSTATE ){
      assert( !yyendofinput );  /* Impossible to shift the $ token */
      yy_shift(yypParser,yyact,yymajor,&yyminorunion);
      yypParser->yyerrcnt--;
      yymajor = YYNOCODE;
    }else if( yyact < YYNSTATE + YYNRULE ){
      yy_reduce(yypParser,yyact-YYNSTATE);
    }else{
      assert( yyact == YY_ERROR_ACTION );
#ifdef YYERRORSYMBOL
      int yymx;
#endif
#ifndef NDEBUG
      if( yyTraceFILE ){
        fprintf(yyTraceFILE,"%sSyntax Error!\n",yyTracePrompt);
      }
#endif
#ifdef YYERRORSYMBOL
      /* A syntax error has occurred.
      ** The response to an error depends upon whether or not the
      ** grammar defines an error token "ERROR".  
      **
      ** This is what we do if the grammar does define ERROR:
      **
      **  * Call the %syntax_error function.
      **
      **  * Begin popping the stack until we enter a state where
      **    it is legal to shift the error symbol, then shift
      **    the error symbol.
      **
      **  * Set the error count to three.
      **
      **  * Begin accepting and shifting new tokens.  No new error
      **    processing will occur until three tokens have been
      **    shifted successfully.
      **
      */
      if( yypParser->yyerrcnt<0 ){
        yy_syntax_error(yypParser,yymajor,yyminorunion);
      }
      yymx = yypParser->yystack[yypParser->yyidx].major;
      if( yymx==YYERRORSYMBOL || yyerrorhit ){
#ifndef NDEBUG
        if( yyTraceFILE ){
          fprintf(yyTraceFILE,"%sDiscard input token %s\n",
             yyTracePrompt,yyTokenName[yymajor]);
        }
#endif
        yy_destructor(yypParser, (YYCODETYPE)yymajor,&yyminorunion);
        yymajor = YYNOCODE;
      }else{
         while(
          yypParser->yyidx >= 0 &&
          yymx != YYERRORSYMBOL &&
          (yyact = yy_find_reduce_action(
                        yypParser->yystack[yypParser->yyidx].stateno,
                        YYERRORSYMBOL)) >= YYNSTATE
        ){
          yy_pop_parser_stack(yypParser);
        }
        if( yypParser->yyidx < 0 || yymajor==0 ){
          yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
          yy_parse_failed(yypParser);
          yymajor = YYNOCODE;
        }else if( yymx!=YYERRORSYMBOL ){
          YYMINORTYPE u2;
          u2.YYERRSYMDT = 0;
          yy_shift(yypParser,yyact,YYERRORSYMBOL,&u2);
        }
      }
      yypParser->yyerrcnt = 3;
      yyerrorhit = 1;
#elif defined(YYNOERRORRECOVERY)
      /* If the YYNOERRORRECOVERY macro is defined, then do not attempt to
      ** do any kind of error recovery.  Instead, simply invoke the syntax
      ** error routine and continue going as if nothing had happened.
      **
      ** Applications can set this macro (for example inside %include) if
      ** they intend to abandon the parse upon the first syntax error seen.
      */
      yy_syntax_error(yypParser,yymajor,yyminorunion);
      yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
      yymajor = YYNOCODE;
      
#else  /* YYERRORSYMBOL is not defined */
      /* This is what we do if the grammar does not define ERROR:
      **
      **  * Report an error message, and throw away the input token.
      **
      **  * If the input token is $, then fail the parse.
      **
      ** As before, subsequent error messages are suppressed until
      ** three input tokens have been successfully shifted.
      */
      if( yypParser->yyerrcnt<=0 ){
        yy_syntax_error(yypParser,yymajor,yyminorunion);
      }
      yypParser->yyerrcnt = 3;
      yy_destructor(yypParser,(YYCODETYPE)yymajor,&yyminorunion);
      if( yyendofinput ){
        yy_parse_failed(yypParser);
      }
      yymajor = YYNOCODE;
#endif
    }
  }while( yymajor!=YYNOCODE && yypParser->yyidx>=0 );
  return;
}
