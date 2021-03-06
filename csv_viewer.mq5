#property script_show_inputs
#define OP_BUY 0           //Buy 
#define OP_SELL 1          //Sell 
#define OP_BUYLIMIT 2      //BUY LIMIT 
#define OP_SELLLIMIT 3     //SELL LIMIT 
#define OP_BUYSTOP 4       //BUY STOP 
#define OP_SELLSTOP 5      //SELL STOP 

#include <WinUser32.mqh>
input string fileName = "input.csv";
input int timeZoneOffset = 3;

void OnStart(void)
  {
   long currentChartId = ChartID();
  
   int f = FileOpen(fileName ,FILE_READ|FILE_ANSI|FILE_CSV, ',');   
   if (f == -1)
   {
      int res = GetLastError();
      MessageBox("File open error! " + (string)res, "Error", MB_OK|MB_ICONWARNING);
      return;
   }
   FileSeek(f, 0, SEEK_SET); 

   int count = 1;
   while(!FileIsLineEnding(f)) {
      string s = FileReadString(f);
   }
   ObjectsDeleteAll(currentChartId);
   
   while(!FileIsEnding(f))
   {
      string open_date = FileReadString(f);
      string close_date = FileReadString(f);
      string symb = FileReadString(f);
      string action = FileReadString(f);
      double openPrice = FileReadNumber(f);
         
      double sl = FileReadNumber(f);
      double tp = FileReadNumber(f);
      string channel = FileReadString(f);
      double closePrice = FileReadNumber(f);
      double diff = FileReadNumber(f);
      double signalPrice = FileReadNumber(f);
      
      while(!FileIsLineEnding(f)) FileReadString(f);
      
      if (StringSubstr(symb, 0, 6) != StringSubstr(Symbol(), 0, 6)) continue;
      
      int ordType = getOrderTypeFromString(action);
      if (ordType == -1)
      {
         Print("Not supported order type: ", action);
         continue;         
      }

      //if (ordType > OP_SELL) closePrice = openPrice; 
      
      datetime openTime = convertDate(open_date);
      datetime closeTime = convertDate(close_date);

      color c = Blue;
      string objName = action + " " + channel+ " " + (string)count;
      
      if (ordType == OP_SELL || ordType == OP_SELLLIMIT || ordType == OP_SELLSTOP)
      {
         c = Red;
         ObjectCreate(currentChartId, objName, OBJ_ARROW_SELL, 0, openTime, openPrice);
      }
      else{
      
      ObjectCreate(currentChartId, objName, OBJ_ARROW_BUY, 0, openTime, openPrice);
      }
 
      ObjectSetInteger(currentChartId, objName, OBJPROP_COLOR, c);
      
      
      objName = "sl-" + action + "-" + (string)count;
      ObjectCreate(currentChartId, objName, OBJ_TREND, 0, openTime, sl, closeTime, sl);
      ObjectSetInteger(currentChartId, objName, OBJPROP_COLOR, Red);
      ObjectSetInteger(currentChartId, objName, OBJPROP_STYLE, STYLE_DASH);
      ObjectSetInteger(currentChartId, objName, OBJPROP_WIDTH, 1);
      ObjectSetInteger(currentChartId, objName, OBJPROP_RAY, false);
      
      objName = "signalPrice-" + action + "-" + (string)count;
      ObjectCreate(currentChartId, objName, OBJ_TREND, 0, openTime, signalPrice, closeTime, signalPrice);
      ObjectSetInteger(currentChartId, objName, OBJPROP_COLOR, Blue);
      ObjectSetInteger(currentChartId, objName, OBJPROP_STYLE, STYLE_SOLID);
      ObjectSetInteger(currentChartId, objName, OBJPROP_WIDTH, 1);
      ObjectSetInteger(currentChartId, objName, OBJPROP_RAY, false);

      objName = "L-" + action + "-" + (string)count;
      ObjectCreate(currentChartId, objName, OBJ_TREND, 0, openTime, openPrice, closeTime, closePrice);
      ObjectSetInteger(currentChartId, objName, OBJPROP_STYLE, STYLE_SOLID);
      ObjectSetInteger(currentChartId, objName, OBJPROP_WIDTH, 2);
      ObjectSetInteger(currentChartId, objName, OBJPROP_RAY, false);
      
      if (diff >0){
         ObjectSetInteger(currentChartId, objName, OBJPROP_COLOR, Green);      
      }
      else{
         ObjectSetInteger(currentChartId, objName, OBJPROP_COLOR, Red);  
      
      }
/*
      objName = "C-" + action + "-" + count;
      ObjectCreate(objName, OBJ_ARROW, 0, closeTime, closePrice);
      
      if (diff >0){
         ObjectSet(objName, OBJPROP_COLOR, Green);      
      }
      else{
         ObjectSet(objName, OBJPROP_COLOR, Red);  
      
      }*/
      count++;   
   }
   FileClose(f);
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
   
   string dateString;
   StringConcatenate(dateString, y, ".", m, ".", d, " ", h, ":", n);
   datetime res = StringToTime(dateString) +timeZoneOffset*3600 + TimeDaylightSavings();   
         
   return(res);
}