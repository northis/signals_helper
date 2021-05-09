#property show_inputs

#include <WinUser32.mqh>
extern string fileName = "viewer.input.csv";
extern int hourOffset = 0; // 0 for UTC

int start()
  {
   int f = FileOpen(fileName ,FILE_READ|FILE_CSV, ',');   
   if (f == -1)
   {
      int res = GetLastError();
      MessageBox("File open error! " + res, "Error", MB_OK|MB_ICONWARNING);
      return(0);
   }
   FileSeek(f, 0, SEEK_SET); 

   int count = 0;
   //header
   while(!FileIsLineEnding(f)) {
      string s = FileReadString(f);
   }
   ObjectsDeleteAll();
   
   while(!FileIsEnding(f))
   {
      int ticket = count;

      string open_date = FileReadString(f);
      string close_date = FileReadString(f);
      string symb = FileReadString(f);
      string action = FileReadString(f);
         
      double sl = FileReadNumber(f);
      double tp = FileReadNumber(f);
      double openPrice = FileReadNumber(f);
      double closePrice = FileReadNumber(f);
      
      while(!FileIsLineEnding(f)) FileReadString(f);
      
      if (StringSubstr(symb, 0, 6) != StringSubstr(Symbol(), 0, 6)) continue;
      
      int ordType = getOrderTypeFromString(action);
      if (ordType == -1)
      {
         Print("Not supported order type: ", action);
         continue;         
      }

      if (ordType > OP_SELL) closePrice = openPrice; 
      
      datetime openTime = convertDate(open_date);
      datetime closeTime = convertDate(close_date);

      color c = Blue;
      if (ordType == OP_SELL || ordType == OP_SELLLIMIT || ordType == OP_SELLSTOP)
         c = Red;

      string objName = "A-" + action + "-" + ticket;
      ObjectCreate(objName, OBJ_ARROW, 0, openTime, openPrice);
      ObjectSet(objName, OBJPROP_COLOR, c);
      ObjectSet(objName, OBJPROP_ARROWCODE, 1);

      objName = "L-" + action + "-" + ticket;
      ObjectCreate(objName, OBJ_TREND, 0, openTime, openPrice, closeTime, closePrice);
      ObjectSet(objName, OBJPROP_STYLE, STYLE_DOT);
      ObjectSet(objName, OBJPROP_RAY, false);
      ObjectSet(objName, OBJPROP_COLOR, c);

      objName = "C-" + action + "-" + ticket;
      ObjectCreate(objName, OBJ_ARROW, 0, closeTime, closePrice);
      ObjectSet(objName, OBJPROP_COLOR, Goldenrod);
      ObjectSet(objName, OBJPROP_ARROWCODE, 3);
      count++;   
   }
   FileClose(f);
   
   MessageBox("Loaded " + count + " orders.", "Finish", MB_OK);   
   return(0);
  }

int getOrderTypeFromString(string s)
{
   if (s == "buy")         return(OP_BUY);
   if (s == "sell")         return(OP_SELL);
   
   return(-1);
}

datetime convertDate(string s)
{
   //2021-02-09T09:32:00+00:00
   string y = StringSubstr(s, 0, 4);
   string m = StringSubstr(s, 5, 2);
   string d = StringSubstr(s, 8, 2);
   string h = StringSubstr(s, 11, 2);
   string n = StringSubstr(s, 14, 2);
   
   datetime res = StrToTime(StringConcatenate(y, ".", m, ".", d, " ", h, ":", n));
   if (hourOffset != 0)
      res = res + hourOffset * 60 * 60; 
      
   return(res);
}