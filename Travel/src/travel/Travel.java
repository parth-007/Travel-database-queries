/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package travel;

/**
 *
 * @author ADMIN
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.*;
import java.sql.*;
import java.util.*;

class DaemonThread implements Runnable{

    public void run()
    {
        while(true)
        {
         //   callme();
        }
    }
    public void init()
    {
    }
  
}


public class Travel extends Thread implements Runnable{

    /**
     * @param args the command line arguments
     */
    
    public void run()
    {
        while(true)
        {
            try {
                callme();
            } catch (InterruptedException ex) {
                Logger.getLogger(Travel.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    public void callme() throws InterruptedException 
    {   
        try{
        Thread.sleep(120000);
          Class.forName("org.postgresql.Driver");
                        con=DriverManager.getConnection(url, user, password);
                        
                        Statement s5  = con.createStatement();
ResultSet rs1 = s5.executeQuery(
"select c.cityid,c.cityname,no_of_visits as visits from city c join (select city.cityid,count(cityid) as no_of_visits from group_package_hotel gph join package pack on gph.packageid=pack.packageid join city on pack.destination_city=city.cityid group by cityid) city_count on c.cityid=city_count.cityid join (select max(no_of_visits) max_city from (select city.cityid,count(cityid) as no_of_visits from group_package_hotel gph join package pack on gph.packageid=pack.packageid join city on pack.destination_city=city.cityid group by cityid) city_count_max ) m on m.max_city=city_count.no_of_visits ");
System.out.println("City with Maximum Visits:");
                       while(rs1.next())
                        {
                            int id = rs1.getInt(1);
                            String ss1 = rs1.getString(2);
                            int rate1 = rs1.getInt(3);
                            
                            System.out.println(id + " " + ss1 + " " + rate1);
                        }
        }
        catch(Exception s1)
        {
            
        }
    }
        Connection con;

   public static String url="jdbc:postgresql://127.0.0.1:5433/TravelDB";
    public static String user="postgres";
    public static String password="admin123";
     public Connection dbConnection(){
        try{
        Class.forName("org.postgresql.Driver");
        }catch(ClassNotFoundException ex)
        {
            ex.getMessage();
        }
        try {
            DriverManager.getConnection(url, user, password);
System.out.println("Connect");
                    } catch (SQLException ex) {
            Logger.getLogger(Travel.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Connection Failed");
        }
        return con;
    }

//
    public static void main(String[] args) throws SQLException, IOException {
        // TODO code application logic here
        InputStreamReader ip = new InputStreamReader(System.in);
        BufferedReader bR = new BufferedReader(ip);
        
        
        Connection con1;
        con1=DriverManager.getConnection(url, user, password);
        Thread dt = new Thread(new Travel());
        dt.setDaemon(true);
        dt.start();
        String str1;
        Scanner sc = new Scanner(System.in);
        int ch=-1;
        do
        {
            try {
                System.out.println("======================================================================================================");
                System.out.println("Welcome to AllStar Travels.");
                System.out.println("1.Find all agencies with their respective cities.");
                System.out.println("2.Find out the hotel which has maximum ratings in given city(KASHMIR,DAMAN,JAIPUR).");
                System.out.println("3.Find out the offer that's been used the most.");
                System.out.println("4.Find out the passengers who have used all packages.(Division operator).");
                System.out.println("5.Show the package information along with the no_of_times they have been availed (in descending order).");
                System.out.println("6.Show all the offers those are currently active.");
                System.out.println("7.Display passenger information for passenger who has given a lengthy feedback(at least of length 50 characters) along with the feedback message.");
                System.out.println("8.Find out total Male and Female Passengers.");
                System.out.println("9.Show Passenger information based on group id");
                System.out.println("10.Insert A Hotel");
                System.out.println("11.Update any Offer");
                System.out.println("12.Delete Offer");
                System.out.println("13.Exit");
                System.out.println("Enter Your Choice:");
                ch=sc.nextInt();
                switch(ch)
                {
                    case 1:
                        Class.forName("org.postgresql.Driver");
                        
                        Statement s1 = con1.createStatement();
ResultSet rs = s1.executeQuery("select cityname,agency.name from city join agency on city.agencyid=agency.agencyid");
                          
                        if(rs==null)
                        {
                            System.out.println("No Such Records");
                        }
                        else
                        {
                            System.out.printf("%-15s \t %-20s \n","City","Agency");
                            System.out.println("--------------------------------------------------");
                        while(rs.next())
                        {
                               String city = rs.getString(1);
                               String agency = rs.getString(2);
                               System.out.printf("%-15s \t %-20s\n",city,agency);
                        }
                        }
                        rs.close();
                        s1.close();
                        break;
                    case 2:
                        Class.forName("org.postgresql.Driver");
                        
                        System.out.println("Enter Cityname");
                        str1=sc.next();
                        Statement s2 = con1.createStatement();
                        ResultSet rs1 = s2.executeQuery("SELECT H1.HOTEL_NAME,H1.RATINGS FROM HOTEL H1 INNER JOIN (SELECT CITYID,MAX(RATINGS) RATINGS FROM HOTEL GROUP BY CITYID) AS H2 ON \n" +
"H1.CITYID=H2.CITYID AND H1.RATINGS=H2.RATINGS JOIN CITY ON H1.CITYID=CITY.CITYID AND CITY.CITYNAME='"+str1+"'");
                        while(rs1.next())
                        {
                            String hotelname = rs1.getString(1);
                            int ratings = rs1.getInt(2);
                            System.out.printf("%-20s %10d \n",hotelname,ratings);
                            
                        }
                        rs1.close();
                        s2.close();
                        break;
                    case 3:
                        Class.forName("org.postgresql.Driver");
                        
                        Statement s3 = con1.createStatement();
                        ResultSet rs3 = s3.executeQuery("select * from offer join (select offercode from offer left join passenger_offer on passenger_offer.offerid=offer.offerid group by offer.offercode order by count(*) desc limit 1) as myoffer on offer.offercode=myoffer.offercode");
                        

                        while(rs3.next())
                        {
                               int offerid = rs3.getInt(1);
                               String ocode = rs3.getString(2);
                               String sdate = rs3.getString(3);
                               String edate = rs3.getString(4);
                               String oname = rs3.getString(5);
                               String desc = rs3.getString(6);
                               int discount = rs3.getInt(7);
                               
System.out.printf("%5d %-10s %-15s %-20s %-25s %-30s %10d\n",offerid,ocode,sdate,edate,oname,desc,discount);
                        }
                        rs3.close();
                        s3.close();
                        break;
                    case 4:
                        Class.forName("org.postgresql.Driver");
                        
                        Statement s6 = con1.createStatement();
ResultSet rs6 = s6.executeQuery("select * from passenger join (select distinct(groupid) from passenger where groupid in (select groupid from group_package_hotel except (select pg.groupid from (select gph.groupid,p1.packageid from group_package_hotel as gph cross join package p1  except select pg.groupid,pg.packageid from group_package_hotel as pg) as pg))) as r1 on passenger.groupid=r1.groupid ");

                        while(rs6.next())
                        {
                               String fname = rs6.getString(3);
                               String lname = rs6.getString(4);
                               String aadhar = rs6.getString(5);
                               String bdate= rs6.getString(6);
                               String phone= rs6.getString(7);
                               String gender= rs6.getString(8);
                               int inid= rs6.getInt(9);
System.out.println(fname + " " + lname +  " " + aadhar + " " + bdate + " " + phone +  " " + gender  +  " " +inid);
                        }
                        rs6.close();
                        s6.close();
                        break;
                    case 5:
                        Class.forName("org.postgresql.Driver");
                        
                        Statement s10 = con1.createStatement();
 ResultSet rs10 = s10.executeQuery("select p.*,(select count(packageid) as no_of_uses from group_package_hotel as gph where gph.packageid=p.packageid ) from package as p order by no_of_uses desc");

                        while(rs10.next())
                        {
String pname = rs10.getString(2);
String type = rs10.getString(3);
String mode = rs10.getString(4);
int c1=rs10.getInt(5);
int c2=rs10.getInt(6);
int days=rs10.getInt(7);
int nights=rs10.getInt(8);
int price = rs10.getInt(9);
int use = rs10.getInt(10);

System.out.println(pname + " " + type + " " + mode + " " + c1 + " " + c2 +" " + days
+ " " + nights +  " "+ price+  " " + use);

                        }
                        rs10.close();
                        s10.close();
                        break;
                    case 6:
                         Class.forName("org.postgresql.Driver");
                        
                        Statement s55 = con1.createStatement();
                        ResultSet rs55 = s55.executeQuery("Select * from offer where now() between start_date and end_date");
                        

                        while(rs55.next())
                        {
                               int offerid = rs55.getInt(1);
                               String ocode = rs55.getString(2);
                               String sdate = rs55.getString(3);
                               String edate = rs55.getString(4);
                               String oname = rs55.getString(5);
                               String desc = rs55.getString(6);
                               int discount = rs55.getInt(7);
                               
                               System.out.println(offerid + " " + ocode + " " + sdate + " " + edate + " " + oname + " " + desc + " " + discount );
                        }
                        rs55.close();
                        s55.close();
                        break;
                    case 7:
Class.forName("org.postgresql.Driver");
                        
                        Statement s9 = con1.createStatement();
                        ResultSet rs9 = s9.executeQuery("select pass.*,f.msg from passenger as pass join passenger_feedback pf on pass.passengerid=pf.passengerid join feedback as f on pf.feedbackid=f.feedbackid and length(f.msg)>=50");

                        while(rs9.next())
                        {
                            String fname = rs9.getString(2);
                            String lname = rs9.getString(3);
                            String aadharno  = rs9.getString(4);
                            String bdate = rs9.getString(5);
                            String phone = rs9.getString(6);
                            String gender = rs9.getString(7);
                            String msg = rs9.getString(10);
                               
System.out.println(fname +" " + lname + " " + aadharno + " " + bdate+ " " + phone + " " + gender + " " + msg );
                        }
                        rs9.close();
                        s9.close();
                        break;
                    case 8:
                        Class.forName("org.postgresql.Driver");
                        
                        Statement s5 = con1.createStatement();
                        ResultSet rs5 = s5.executeQuery("SELECT COUNT(CASE WHEN GENDER='MALE' THEN 1 END) AS MALE_CNT,COUNT(CASE WHEN GENDER='FEMALE' THEN 1 END) AS FEMALE_CNT FROM PASSENGER");

                        while(rs5.next())
                        {
                               int male = rs5.getInt(1);
                               int female = rs5.getInt(2);
                               System.out.println("Male:-" + male + " " +  "Female:-"+ female);
                        }
                        rs5.close();
                        s5.close();
                        break;
                    case 9:
                        int gid;
                        System.out.println("Enter Groupid:");
                        gid=sc.nextInt();
                        Class.forName("org.postgresql.Driver");
                        
                        Statement s8 = con1.createStatement();
                        ResultSet rs8 = s8.executeQuery("SELECT * FROM PASSENGER WHERE GROUPID='"+gid+"'");

                        while(rs8.next())
                        {
                               int pid = rs8.getInt(1);
                               String fname = rs8.getString(3);
                               String lname = rs8.getString(4);
                               String aadharno = rs8.getString(5);
                               String bdate = rs8.getString(6);
                               String phone = rs8.getString(7);
                               String gender = rs8.getString(8);
                               
                             
  System.out.println(pid + " " + fname + " " + lname + " " + aadharno + " " + bdate + " " + phone + " " + gender );
  
                        }
                        rs8.close();
                        s8.close();

                        break;
                    case 10:
                        String hotelname;
                        String address;
                        int ratings;
                        String cityname;
                        int cnt=-1;
                        
                        System.out.println("Enter City (in Capital):");
                        cityname = sc.next();
                        Statement sps = con1.createStatement();
                        ResultSet rss9 = sps.executeQuery("select cityid from city where cityname='"+cityname+"'");
                        while(rss9.next())
                        {
                            cnt = rss9.getInt(1);
                        }
                        
                        if(cnt==-1)
                        {
                            System.out.println("No such city Exist");
                        }
                        else
                        {
                            System.out.println("Enter Name of Hotel:");
                        hotelname=bR.readLine();
                        System.out.println("Enter Address of Hotel:");
                        address = bR.readLine();
                        System.out.println("Enter Ratings:");
                        ratings = sc.nextInt();
                        
                        PreparedStatement ps;
ps=con1.prepareStatement("insert into hotel(address,ratings, hotel_name,cityid) values ('"+address+"','"+ratings+"','"+hotelname+"','"+cnt+"')");
                        ps.executeUpdate();
                        System.out.println("Insertion Successful");
                        }
                        
                        break;
                    case 11:
                        int oid;
                        int ccc=-1;
                        String ocode;
                        String sdate;
                        String edate;
                        String oname;
                        String desc;
                        int discount;
                        int count1=-1;
                        System.out.println("Enter Offerid to Update(11001-11007):");
                        oid = sc.nextInt();
                        
                        Statement sp1 = con1.createStatement();
                        ResultSet sp11=sp1.executeQuery("select count(*) as cnt from offer where offerid='"+oid+"'");
                        while(sp11.next())
                        {
                            count1 = sp11.getInt(1);
                        }
                        if(count1==1)
                        {
                            System.out.println("Enter Description to change:");
                            desc = bR.readLine();
                            System.out.println("Enter Startdate to change:");
                            sdate = bR.readLine();
                            System.out.println("Enter EndDate to chage:");
                            edate = bR.readLine();
                            System.out.println("Enter Offername to change:");
                            oname = bR.readLine();
                            System.out.println("Enter discount rate to change:");
                            discount=sc.nextInt();
                           PreparedStatement sx;
                           sx=con1.prepareStatement("update offer set description='"+desc+"',start_date='"+sdate+"',end_date='"+edate+"',offer_name='"+oname+"',discount='"+discount+"' where offerid='"+oid+"'");
                           sx.executeUpdate();
                           System.out.println("Updation Successful");
                        }
                        else
                        {
                            System.out.println("No such offer Exist");
                        }
                        break;
                    case 12:
                        int offeridid;
                        int count2=-1;
                        System.out.println("Enter Offerid to Update(11001-11007):");
                        oid = sc.nextInt();
                        
                        Statement spz1 = con1.createStatement();
                        ResultSet spz11=spz1.executeQuery("select count(*) as cnt from offer where offerid='"+oid+"'");
                        while(spz11.next())
                        {
                            count2 = spz11.getInt(1);
                        }
                        if(count2==1)
                        {
                               PreparedStatement sxx;
                           sxx=con1.prepareStatement("delete from offer where offerid='"+oid+"'");
                           sxx.executeUpdate();
                           System.out.println("Deletion Successful");
                        }
                        else
                        {
                            System.out.println("Such offer does not exist");
                        }
                        break;
                    case 13:
                        break;
                       
                }
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(Travel.class.getName()).log(Level.SEVERE, null, ex);
            }
        }while(ch!=13);


    }

}
