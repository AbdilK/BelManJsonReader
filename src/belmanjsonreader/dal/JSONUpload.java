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

public class JSONUpload
{

    private DBConnectionProvider db;

    public JSONUpload() throws IOException
    {
        db = new DBConnectionProvider();
    }

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

    public void findJSONFolder(File folder) throws IOException, FileNotFoundException, ParseException, SQLException
    {
        for (File listFile : folder.listFiles())
        {
            String path = listFile.getPath();
            uploaderJSON(path);
        }
    }

    public int uploadProdOrderDB(String orderNumber, String customerName, Date deliveryDate, Connection con) throws SQLException
    {
        PreparedStatement ppst = con.prepareStatement("INSERT INTO ProdOrder VALUES (?,?,?)", Statement.RETURN_GENERATED_KEYS);

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

    
      public boolean searchForExistingTask(Connection con, String departmentName, int id) throws SQLException
    {
        boolean existingTask = false;
        PreparedStatement ppst1 = con.prepareStatement("SELECT ProductionID, DepartmentName FROM DepTask "
                                                     + "WHERE DepartmentName = (?) AND ProductionID = (?)");
        ppst1.setString(1, departmentName);
        ppst1.setInt(2, id);

        ResultSet rs = ppst1.executeQuery();
        if (rs.next())
        {
            existingTask = true;
        }
        return existingTask;
    }

    public boolean searchForExistingOrder(Connection con, String orderNumber) throws SQLException
    {
        boolean existingOrder = false;
        PreparedStatement ppst = con.prepareStatement("SELECT OrderNumber FROM ProdOrder WHERE OrderNumber = (?)");
        ppst.setString(1, orderNumber);

        ResultSet rs = ppst.executeQuery();

        if (rs.next())
        {
            existingOrder = true;
        }
        return existingOrder;
    }

    private Date dateFormat(String dateString)
    {
        Long milli = Long.parseLong(dateString.substring(6, dateString.indexOf("+")));
        Date newDate = new Date(milli);
        return newDate;
    }

  
}
