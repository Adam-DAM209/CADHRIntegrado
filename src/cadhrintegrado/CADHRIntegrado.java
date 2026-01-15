/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cadhrintegrado;

import java.sql.CallableStatement;
import pojoshr.ExcepcionHR;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import jdk.internal.org.objectweb.asm.Type;
import pojoshr.*;

/**
 *
 * @author DAM209
 */
public class CADHRIntegrado {

    private Connection conexion;
    
    private String HOST = "jdbc:oracle:thin:@172.16.209.1:1521:test";
    private String USERBD = "HR";
    private String PASSWORD = "kk";

    public CADHRIntegrado() throws ExcepcionHR {
        try {

            System.out.println("Conexion");
            Class.forName("oracle.jdbc.driver.OracleDriver");

        } catch (ClassNotFoundException ex) {
            ExcepcionHR e = new ExcepcionHR();
            e.setMensajeErrorBD(ex.getMessage());
            e.setMensajeErrorUsuario("Error general del sistema. Consulte con el administrador");
            throw e;
        }
    }

    private void conectarBD() throws ExcepcionHR {
        try {

            conexion = DriverManager.getConnection(HOST, USERBD, PASSWORD);

        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();
            e.setMensajeErrorUsuario("Error general del sistema. Consulte con el administrador");
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            throw e;
        }
    }



    /**
     * Elimina un único registro de la tabla REGIONS
     *
     * @param regionId Identificador de región del registro que se desea
     * eliminar
     * @return Cantidad de registros eliminados
     * @throws pojoshr.ExcepcionHR Se lanzará cuando se produzca un error de
     * base de datos
     * @author Ignacio Fontecha
     * @version 1.0
     * @since 11/12/2025 DD/MM/YYYY
     */
    public Integer eliminarRegion(Integer regionId) throws ExcepcionHR {
        int registrosAfectados = 0;
        String dml = "";
        try {
            conectarBD();
            Statement sentencia = conexion.createStatement();
            dml = "DELETE REGIONS WHERE region_id = " + regionId;
            registrosAfectados = sentencia.executeUpdate(dml);
            sentencia.close();
            conexion.close();
        } catch (SQLException ex) {

            ExcepcionHR e = new ExcepcionHR();

            switch (ex.getErrorCode()) {
                case 2292:
                    e.setMensajeErrorUsuario("No se puede eliminar la región porque tiene países asociados");
                    break;
                default:
                    e.setMensajeErrorUsuario("Error general del sistema. Consulte con el administrador");
                    break;
            }

            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dml);

            throw e;
        }

        return registrosAfectados;
    }

    

    /**
     * Este metodo
     *
     * @param departmentId
     * @param department
     * @author Ignacio Fontecha
     * @return
     * @throws ExcepcionHR
     */
    public Integer modificarDepartment(Integer departmentId, Department department) throws ExcepcionHR {
        int registrosAfectados = 0;
        String dml = "UPDATE departments SET department_name=?, manager_id=?, location_id=? WHERE department_id=?";
        try {

            PreparedStatement sentenciaPreparada = conexion.prepareStatement(dml);

            sentenciaPreparada.setString(1, department.getDepartmentName());
            sentenciaPreparada.setObject(2, department.getManager().getEmployeeId(), Type.INT);
            sentenciaPreparada.setObject(3, department.getLocation().getLocationId(), Type.INT);
            sentenciaPreparada.setObject(4, departmentId, Type.INT);
            registrosAfectados = sentenciaPreparada.executeUpdate();

            sentenciaPreparada.close();
            conexion.close();

        } catch (SQLException ex) {

            ExcepcionHR e = new ExcepcionHR();

            switch (ex.getErrorCode()) {
                case 1407:
                    e.setMensajeErrorUsuario("El nombre de departamento es obligatorio");
                    break;
                case 2291:
                    e.setMensajeErrorUsuario("No se ha podido modificar debido a que el empleado o localización no existen");
                    break;
                default:
                    e.setMensajeErrorUsuario("Error general del sistema. Consulte con el administrador");
                    break;
            }

            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dml);

            throw e;
        }

        return registrosAfectados;
    }

    /**
     *
     * @return Un ArrayList de tipo Location
     * @throws ExcepcionHR
     * @author Ignacio Fontecha
     * @version 1.0
     * @since 11/12/2025
     */
    public ArrayList<Location> leerLocations() throws ExcepcionHR {
        ArrayList listaLocations = new ArrayList();
        Location l;
        Country c;
        Region r;
        String dql = "SELECT * FROM regions r, countries c, locations l WHERE r.region_id = c.region_id and c.country_id = l.country_id";
        try {
            conectarBD();
            Statement sentencia = conexion.createStatement();

            ResultSet resultado = sentencia.executeQuery(dql);
            while (resultado.next()) {
                l = new Location();
                l.setLocationId(resultado.getInt("location_id"));
                l.setStreetAdress(resultado.getString("street_address"));
                l.setPostalCode(resultado.getString("postal_code"));
                l.setCity(resultado.getString("city"));
                l.setStateProvince(resultado.getString("state_province"));

                c = new Country();
                c.setCountryId(resultado.getString("country_id"));
                c.setCountryName(resultado.getString("country_name"));

                l.setCountry(c);

                listaLocations.add(l);
            }
            resultado.close();

            sentencia.close();
            conexion.close();

        } catch (SQLException ex) {

            ExcepcionHR e = new ExcepcionHR();

            e.setMensajeErrorUsuario("Error general del sistema. Consulte con el administrador");
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dql);

            throw e;
        }
        return listaLocations;
    }

    
    /**
     * Este método elimina un registro de la tabla Country, según el ID de
     * Country introducido
     *
     * @param countryId Identificador del registro a eliminar
     * @return Devuelve 1 si se ha eliminado el registro con éxito, de lo
     * contrario 0
     * @throws ExcepcionHR Se lanzará cuando ocurra un error de la base de datos
     * @author Adam Janah Benyoussef
     * @version 1.0
     * @since 17/12/2025 DD/MM/YYYY
     */
    public Integer eliminarCountry(String countryId) throws ExcepcionHR {
        int registrosAfectados = 0;
        String dml = "";
        try {
            conectarBD();
            Statement sentencia = conexion.createStatement();
            dml = "DELETE COUNTRIES WHERE country_id = '" + countryId + "'";
            registrosAfectados = sentencia.executeUpdate(dml);
            sentencia.close();
            conexion.close();
        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dml);
            
            switch (ex.getErrorCode()) {
                case 2292:
                    e.setMensajeErrorUsuario("No se puede eliminar debido a que el pais tiene localidades asociadas");
                    break;
                default:
                    e.setMensajeErrorUsuario("Error general del sistema. Consulte con el administrador");
                    break;
            }

            throw e;
        }
        return registrosAfectados;
    }

    /**
     * Este método introduce un nuevo registro en la tabla Employees
     *
     * @param employee Objeto Employee a insertar en la tabla
     * @return Devuelve 1 en caso de inserción exitosa, de lo contrario 0
     * @throws ExcepcionHR Se lanzará cuando ocurra un error en la base de datos
     * @author Adam Janah Benyoussef
     * @version 1.0
     * @since 17/12/2025 DD/MM/YYYY
     */
    public Integer insertarEmployee(Employee employee) throws ExcepcionHR {
        conectarBD();
        int registrosAfectados = 0;
        String dml = "INSERT INTO hr.employees (employee_id,first_name,last_name,email,phone_number,hire_date,job_id,salary,commission_pct,manager_id,department_id) "
                + "VALUES "
                + "(SECUENCIA_EMPLOYEE_ID.nextval,?,?,?,?,?,?,?,?,?,?)";

        try {
            PreparedStatement sentencia = conexion.prepareStatement(dml);

            sentencia.setString(1, employee.getFirstName());
            sentencia.setString(2, employee.getLastName());
            sentencia.setString(3, employee.getEmail());
            sentencia.setString(4, employee.getPhoneNumber());
            sentencia.setObject(5, employee.getHireDate());
            sentencia.setString(6, employee.getJobId().getJobId());
            sentencia.setObject(7, employee.getSalary(), java.sql.Types.FLOAT);
            sentencia.setObject(8, employee.getCommissionPCT(), java.sql.Types.FLOAT);
            sentencia.setObject(9, employee.getManager().getEmployeeId(), Type.INT);
            sentencia.setObject(10, employee.getDepartmentId().getDepartmentId(), Type.INT);

            registrosAfectados = sentencia.executeUpdate();

            sentencia.close();
            conexion.close();

        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();
            
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dml);
            switch (ex.getErrorCode()) {
                case 1:
                    e.setMensajeErrorUsuario("El email introducido ya existe");
                    break;
                case 1400:
                    e.setMensajeErrorUsuario("Los campos de Email, Apellido, Fecha de contratación y Trabajo son obligatorios");
                    break;
                case 2290: //Check Constraint de salario
                    e.setMensajeErrorUsuario("El salario debe ser mayor que 0");
                    break;
                case 2291:
                    e.setMensajeErrorUsuario("El Departamento, Trabajo o Jefe indicados no existen");
                    break;
                default:
                    e.setMensajeErrorUsuario("Error general del sistema. Consulte con el administrador");
                    break;
            }

            throw e;
        } catch (Exception e) {
            System.out.println(e);

        }

        return registrosAfectados;
    }

    ////////////////////////////////////////////
    //////////////SEGUIR DEBAJO/////////////////
    ////////////////////////////////////////////
    
    
    
}
