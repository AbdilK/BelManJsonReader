package belmanjsonreader.dal;


import belmanjsonreader.bll.DBConnectionProvider;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.Date;
import java.util.Timer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Qash
 */
public class JSONUpload
{
    
    private DBConnectionProvider db;

    /**
     * Her oprettes der forbindelse til databasen fra DBConnectionprovider filen
     * @throws IOException kaster IOException hvis der opstår IO fejl
     */
    public JSONUpload() throws IOException
    {
        db = new DBConnectionProvider();
    }

    /**
     * ScheduledExecutorService bruges til at finde JSON filen og opdatere tabellerne i databsen
     * Der er en scheduled delay på 15 sekunder, som går ind og opdatere tabellerne i databasen for hver 15. sekund.
     * @param args
     * @throws SQLException kaster SQLEXception hvis der opstår SQL fejl
     * @throws IOException kaster IOEXception hvis der opstår IO fejl
     * @throws FileNotFoundException kaster FileNotFoundException hvis filen ikke kan findes
     */
    public static void main(String[] args) throws SQLException, IOException, FileNotFoundException
    {
        ScheduledExecutorService exe = Executors.newScheduledThreadPool(1);
        JSONUpload useJSON = new JSONUpload();

        Runnable task = () ->
        {
            try
            {
                useJSON.findJSONFolder(new File("JSON"));
            } catch (IOException | ParseException | SQLException ex)
            {
                Logger.getLogger(JSONUpload.class.getName()).log(Level.SEVERE, null, ex);
            }
        };

        exe.scheduleWithFixedDelay(task, 0, 15, TimeUnit.SECONDS);
    }

    /**
     * Nedenstående kode indsætter alt data fra JSON filen ind i vores database
     * @param path finder JSON filens filsti
     * @throws FileNotFoundException kaster FileNotFoundException hvis JSON filen ikke kan findes
     * @throws IOException kaster IOException hvis IO fejl opstår
     * @throws ParseException kaster ParseException hvis der ikke kan parses
     * @throws SQLException kaster SQLException hvis der opstår SQL fejl
     */
    public void uploaderJSON(String path) throws FileNotFoundException, IOException, ParseException, SQLException
    {
        Object obj = new JSONParser().parse(new FileReader(path));

        JSONObject jObject = (JSONObject) obj;

        try (Connection con = db.getConnection())
        {
            int id = 0;
            
            JSONArray orderArray = (JSONArray) jObject.get("ProductionOrders");
            
            for (Object object : orderArray)
            {
                JSONObject oObject = (JSONObject) object;

                JSONObject pObject = (JSONObject) oObject.get("Customer");
                String customerName = (String) pObject.get("Name");

                JSONObject orderObject = (JSONObject) oObject.get("Order");
                String orderNumber = (String) orderObject.get("OrderNumber");

                JSONObject deliveryObject = (JSONObject) oObject.get("Delivery");
                String deliveryDateToString = (String) deliveryObject.get("DeliveryTime");
                Date deliveryDate = dateFormat(deliveryDateToString);

                if (!searchForExistingOrder(con, orderNumber))
                {
                    id = uploadProdOrderDB(orderNumber, customerName, deliveryDate, con);
                }

                JSONArray depArray = (JSONArray) oObject.get("DepartmentTasks");
                for (Object object1 : depArray)
                {
                    JSONObject depObject = (JSONObject) object1;

                    JSONObject dObject = (JSONObject) depObject.get("Department");
                    String departmentName = (String) dObject.get("Name");

                    String endDateToString = (String) depObject.get("EndDate");
                    Date endDate = dateFormat(endDateToString);

                    String startDateToString = (String) depObject.get("StartDate");
                    Date startDate = dateFormat(startDateToString);

                    boolean finishedTask = (boolean) depObject.get("FinishedOrder");

                    if (id > 0 && !searchForExistingTask(con, departmentName, id))
                    {
                        uploadDepTaskDB(departmentName, startDate, endDate, finishedTask, 0, con, id);
                    }

                }

            }
        }
    }

    /**
     * Finder JSON filens mappe og printer samtidig en kommentar ud, hvor hvergang tabellen opdateres.
     * @param folder
     * @throws IOException kaster IOException hvis IO fejl opstår
     * @throws FileNotFoundException kaster FileNotFoundException hvis JSON filen ikke kan findes
     * @throws ParseException kaster ParseException hvis der ikke kan parses
     * @throws SQLException kaster SQLException hvis der opstår SQL fejl
     */
    public void findJSONFolder(File folder) throws IOException, FileNotFoundException, ParseException, SQLException
    {
        for (File listFile : folder.listFiles())
        {
            String path = listFile.getPath();
            uploaderJSON(path);
            System.out.println("JSON has been loaded succesfully");
            System.out.println("Updating again in 15 seconds");
            System.out.println("--------------------------------");
        }
        
    }

    /**
     * Indsætter data fra JSON filen ind i ProdOrder tabellen på databasen
     * @param orderNumber
     * @param customerName
     * @param deliveryDate
     * @param con
     * @return id returnerer ID
     * @throws SQLException kaster SQLException hvis der opstår SQL fejl
     */
    public int uploadProdOrderDB(String orderNumber, String customerName, Date deliveryDate, Connection con) throws SQLException
    {

        PreparedStatement ppst = con.prepareStatement("INSERT INTO ProdOrder VALUES (?,?,?, 'Halvfab')", Statement.RETURN_GENERATED_KEYS);


        java.sql.Date sqlDate = new java.sql.Date(deliveryDate.getTime());
        ppst.setString(1, orderNumber);
        ppst.setString(2, customerName);
        ppst.setDate(3, sqlDate);

        ppst.execute();

        ResultSet rs = ppst.getGeneratedKeys();
        int id = 0;
        if (rs.next())
        {
            id = rs.getInt(1);
        }
        return id;
    }

    /**
     * Indsætter data fra JSON filen ind DepTask tabellen på databasen
     * @param departmentName
     * @param startDate
     * @param endDate
     * @param taskStatus
     * @param estimatedTime
     * @param con
     * @param id
     * @throws SQLException kaster SQLException hvis der opstår SQL fejl
     */
    public void uploadDepTaskDB(String departmentName, Date startDate, Date endDate, boolean taskStatus, int estimatedTime, Connection con, int id) throws SQLException
    {


        PreparedStatement ppst1 = con.prepareStatement("INSERT INTO DepTask VALUES (?,?,?,?,?,?)");


        java.sql.Date sqlStartDate = new java.sql.Date(startDate.getTime());
        java.sql.Date sqlEndDate = new java.sql.Date(endDate.getTime());

        ppst1.setInt(1, id);
        ppst1.setString(2, departmentName);
        ppst1.setDate(3, sqlStartDate);
        ppst1.setDate(4, sqlEndDate);
        ppst1.setInt(5, estimatedTime);
        ppst1.setBoolean(6, taskStatus);

        ppst1.execute();
    }

    
    /**
     * Finder den sidste department som en ordre har været i
     * @param id
     * @param startDate
     * @return
     * @throws SQLException kaster SQLException hvis der opstår SQL fejl
     */
    public String getLastDepartment(int id, java.sql.Date startDate) throws SQLException {
        
        String lastDepartment = "";
        try (Connection con = db.getConnection())
        {
            PreparedStatement ppst1 = con.prepareStatement("SELECT DepartmentName FROM DepTask WHERE ProductionID = ? AND EndDate = ?");
            
            ppst1.setInt(1, id);
            ppst1.setDate(2, startDate);
            
            ResultSet rs = ppst1.executeQuery();
            while(rs.next()) {
                lastDepartment = rs.getString("DepartmentName");
            }
            
        }
        
        return lastDepartment;
    }


    /**
     * Nedenstående kode finder en task, efter ordrens productionsID og productionsNavn
     * @param con
     * @param departmentName
     * @param id
     * @return existingTask
     * @throws SQLException kaster SQLException hvis der opstår SQL fejl
     */
    public boolean searchForExistingTask(Connection con, String departmentName, int id) throws SQLException
    {
        boolean existingTask = false;
        PreparedStatement ppst1 = con.prepareStatement("SELECT ProductionID, DepartmentName FROM DepTask "
                                                     + "WHERE DepartmentName = ? AND ProductionID = ?");
        ppst1.setString(1, departmentName);
        ppst1.setInt(2, id);

        ResultSet rs = ppst1.executeQuery();
        if (rs.next())
        {
            existingTask = true;
        }
        return existingTask;
    }

    /**
     * Nedenstående kode finder en ordre, efter ordrenummer
     * @param con
     * @param orderNumber
     * @return existingOrder
     * @throws SQLException kaster SQLException hvis der opstår SQL fejl
     */
    public boolean searchForExistingOrder(Connection con, String orderNumber) throws SQLException
    {
        boolean existingOrder = false;
        PreparedStatement ppst = con.prepareStatement("SELECT OrderNumber FROM ProdOrder WHERE OrderNumber = ?");
        ppst.setString(1, orderNumber);

        ResultSet rs = ppst.executeQuery();

        if (rs.next())
        {
            existingOrder = true;
        }
        return existingOrder;
    }
    
    /**
     * Nedenstående parser datatypen Long til en string
     * @param dateString
     * @return newDate
     */
    private Date dateFormat(String dateString)
    {
        Long milli = Long.parseLong(dateString.substring(6, dateString.indexOf("+")));
        Date newDate = new Date(milli);
        return newDate;
    }

  
}
