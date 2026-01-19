/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cadhrintegrado;

import java.sql.CallableStatement;
import pojoshr.ExcepcionHR;
import java.sql.Connection;
import java.sql.Date;
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
    /**
     * Inserta un registro en la tabla <code>job_history</code> de la base de
     * datos.
     * <p>
     * Este método utiliza un {@link PreparedStatement} para insertar un
     * historial laboral de un empleado, incluyendo fechas de inicio y fin, el
     * puesto (job) y el departamento asociado.
     * </p>
     * <p>
     * En caso de error, se lanzará una {@link ExcepcionHR} personalizada que
     * contiene información del error de la base de datos, el mensaje SQL y un
     * mensaje amigable para el usuario.
     * </p>
     *
     * @param jobHistory Objeto {@link JobHistory} que contiene la información
     * del historial laboral a insertar, incluyendo:
     * <ul>
     * <li>employeeId: ID del empleado.</li>
     * <li>startDate: Fecha de inicio del puesto.</li>
     * <li>endDate: Fecha de fin del puesto.</li>
     * <li>jobId: Identificador del puesto.</li>
     * <li>departmentId: Identificador del departamento (opcional).</li>
     * </ul>
     * @return El número de registros afectados por la operación de inserción.
     * @throws ExcepcionHR Si ocurre algún error durante la operación de
     * inserción. Los posibles errores incluyen:
     * <ul>
     * <li>1 (ORA-00001): Clave primaria duplicada.</li>
     * <li>2290 (ORA-02290): Violación de restricción CHECK.</li>
     * <li>2291 (ORA-02291): Violación de Foreign Key.</li>
     * <li>Otros códigos SQL: Error no manejado.</li>
     * </ul>
     */
    public Integer insertarJobHistory(JobHistory jobHistory) throws ExcepcionHR {
        conectarBD();
        int registrosAfectados = 0;

        String dml = "INSERT INTO job_history (employee_id, start_date, end_date, job_id, department_id) VALUES (?, ?, ?, ?, ?)";

        try {
            PreparedStatement sentencia = conexion.prepareStatement(dml);

            sentencia.setObject(1, jobHistory.getEmployeeId().getEmployeeId(), Type.INT);
            sentencia.setObject(2, new java.sql.Date(jobHistory.getStartDate().getTime()));
            sentencia.setObject(3, new java.sql.Date(jobHistory.getEndDate().getTime()));
            sentencia.setString(4, jobHistory.getJobId().getJobId());

            if (jobHistory.getDepartmentId().getDepartmentId() != null) {
                sentencia.setInt(5, jobHistory.getDepartmentId().getDepartmentId());
            } else {
                sentencia.setNull(5, java.sql.Types.INTEGER);
            }

            registrosAfectados = sentencia.executeUpdate();
        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();

            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());

            switch (ex.getErrorCode()) {
                case 1:  // ORA-00001
                    e.setMensajeErrorUsuario("La combinacion empleado y fecha de inicio no puede repetirse.");
                    break;
                case 2290: // ORA-02290
                    e.setMensajeErrorUsuario("La fecha de final tiene que ser posterior a la de inicio.");
                    break;
                case 2291: // ORA-02291
                    e.setMensajeErrorUsuario("El empleado, el trabajo o el departamento que ha dicho no existe.");
                    break;
                case 1400: // ORA-02291
                    e.setMensajeErrorUsuario("Todos los datos menos departamento son obligatorios.");
                    break;
                default:
                    e.setMensajeErrorUsuario("Error SQL no manejado: " + ex.getMessage());
                    break;
            }

            throw e;
        }
        return registrosAfectados;
    }

    /**
     * Elimina un departamento de la base de datos HR según el ID proporcionado.
     * <p>
     * Este método se conecta a una base de datos Oracle, ejecuta una sentencia
     * SQL DELETE sobre la tabla DEPARMENTS y retorna la cantidad de registros
     * afectados.
     * </p>
     * <p>
     * En caso de error, se lanzará una {@link ExcepcionHR} personalizada que
     * incluye información sobre el error de la base de datos, la sentencia SQL
     * ejecutada y un mensaje de error amigable para el usuario.
     * </p>
     *
     * @param departmentId El ID del departamento que se desea eliminar.
     * @return El número de registros afectados por la operación de eliminación.
     * @throws ExcepcionHR Si ocurre algún error al cargar el driver de Oracle,
     * al conectarse a la base de datos o al ejecutar la sentencia SQL. Los
     * mensajes de error varían según el tipo de fallo:
     * <ul>
     * <li>1407: Nombre del departamento obligatorio.</li>
     * <li>2292: No se puede eliminar el departamento porque tiene empleados o
     * ubicaciones asociadas.</li>
     * <li>Otros códigos: Error general del sistema.</li>
     * </ul>
     */
    public Integer eliminarDepartamento(Integer departmentId) throws ExcepcionHR {
        int registrosAfectados = 0;
        String dml = "";
        try {
            conectarBD();
            Statement sentencia = conexion.createStatement();

            dml = "DELETE FROM DEPARTMENTS WHERE DEPARTMENT_ID = " + departmentId;
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
                    e.setMensajeErrorUsuario("No se puede eliminar el departamento porque tiene empleados o historiales.");
                    break;
                default:
                    e.setMensajeErrorUsuario("Error general del sistema. Consulte con el administrador.");
                    break;
            }
            throw e;
        }
        return registrosAfectados;
    }

    /**
     * Elimina un único registro de la tabla Jobs
     *
     * @param jobId Identificador de job del registro que se desea eliminar
     * @return Cantidad de registros eliminados
     * @throws pojoshr1.ExcepcionHR Se lanzará cuando se produzca un error de
     * base de datos
     * @author Óscar Eduardo Arango Torres
     * @version 1.0
     * @since AaD 1.0
     */
    public int eliminarJob(String jobId) throws ExcepcionHR {
        conectarBD();
        int registrosAfectados = 0;
        String dml = "DELETE JOBS WHERE job_id = '" + jobId + "'";
        try {
            Statement sentencia = conexion.createStatement();
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
                    e.setMensajeErrorUsuario("No se puede eliminar el trabajo porque tiene empleados o datos históricos asociados");
                    break;

                default:
                    e.setMensajeErrorUsuario("Error general del sistema. Consulta con el administrador");
            }
            throw e;
        }
        return registrosAfectados;
    }

    /**
     * Lee todas los registros de la tabla Jobs
     *
     * @return Cantidad de registros leídos
     * @throws pojoshr1.ExcepcionHR Se lanzará cuando se produzca un error de
     * base de datos
     * @author Óscar Eduardo Arango Torres
     * @version 1.0
     * @since AaD 1.0
     */
    public ArrayList<Job> leerJobs() throws ExcepcionHR {
        conectarBD();
        ArrayList listaJobs = new ArrayList();
        Job j;
        String dql = "select * from JOBS";
        try {
            Statement sentencia = conexion.createStatement();
            ResultSet resultado = sentencia.executeQuery(dql);
            while (resultado.next()) {
                j = new Job();
                j.setJobId(resultado.getString("JOB_ID"));
                j.setJobTitle(resultado.getString("JOB_TITLE"));
                j.setMaxSalary(resultado.getInt("MAX_SALARY"));
                j.setMinSalary(resultado.getInt("MIN_SALARY"));

                listaJobs.add(j);
            }
            resultado.close();
            sentencia.close();
            conexion.close();

        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dql);
            e.setMensajeErrorUsuario("Error general del sistema, consulte con el administrador");
            throw e;
        }
        return listaJobs;
    }

    // --- Insertar Countries
    public Integer insertarCountry(Country country) throws ExcepcionHR {
        // Abrir la conexión
        conectarBD();

        int registrosAfectados = 0;

        // SQL para insertar
        String dml = "INSERT INTO COUNTRIES (COUNTRY_ID, COUNTRY_NAME, REGION_ID) VALUES (?, ?, ?)";

        try {
            // Preparar la sentencia
            PreparedStatement sentenciaPreparada = conexion.prepareStatement(dml);
            sentenciaPreparada.setString(1, country.getCountryId());       // countryId
            sentenciaPreparada.setString(2, country.getCountryName());     // countryName
            sentenciaPreparada.setInt(3, country.getRegion().getRegionId()); // regionId

            // Ejecutar
            registrosAfectados = sentenciaPreparada.executeUpdate();

            // Cerrar recursos
            sentenciaPreparada.close();
            conexion.close();

        } catch (SQLException ex) {
            // Manejar errores con ExcepcionHR
            ExcepcionHR e = new ExcepcionHR();
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dml);

            switch (ex.getErrorCode()) {
                case 1400: // NOT NULL
                    e.setMensajeErrorUsuario("El identificador de país no ha sido proporcionado");
                    break;
                case 1: // UNIQUE constraint violation
                    e.setMensajeErrorUsuario("El identificador del país ya existe");
                    break;
                case 2291: // foreign key violation
                    e.setMensajeErrorUsuario("La región indicada no existe");
                    break;
                default:
                    e.setMensajeErrorUsuario("Error general del sistema, consulte con el administrador");
            }

            throw e;
        }

        // Devolver número de registros insertados
        return registrosAfectados;
    }

    // --- LEER COUNTRIES
    public ArrayList<Country> leerCountries() throws ExcepcionHR {
        // Conectar a la base de datos
        conectarBD();

        // Lista donde guardaremos los objetos Country
        ArrayList<Country> listaCountries = new ArrayList<>();

        // Variables auxiliares
        Country c;
        Region r;

        // SQL para leer todos los países junto con su región
        String dql = "SELECT C.COUNTRY_ID, C.COUNTRY_NAME, R.REGION_ID, R.REGION_NAME "
                + "FROM COUNTRIES C "
                + "INNER JOIN REGIONS R ON C.REGION_ID = R.REGION_ID";

        try {
            // Crear el Statement
            Statement sentencia = conexion.createStatement();

            // Ejecutar la consulta
            ResultSet resultado = sentencia.executeQuery(dql);

            // Recorrer cada fila del resultado
            while (resultado.next()) {
                // Crear el objeto Region
                r = new Region();
                r.setRegionId(resultado.getInt("REGION_ID"));
                r.setRegionName(resultado.getString("REGION_NAME"));

                // Crear el objeto Country y asignar la región
                c = new Country();
                c.setCountryId(resultado.getString("COUNTRY_ID"));
                c.setCountryName(resultado.getString("COUNTRY_NAME"));
                c.setRegion(r);

                // Añadir el Country a la lista
                listaCountries.add(c);
            }

            // Cerrar recursos
            resultado.close();
            sentencia.close();
            conexion.close();

        } catch (SQLException ex) {
            // Capturar y lanzar la excepción personalizada
            ExcepcionHR e = new ExcepcionHR();
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dql);
            e.setMensajeErrorUsuario("Error general del sistema, consulte con el administrador");
            throw e;
        }

        // Devolver la lista completa de countries
        return listaCountries;
    }

    /*
        eliminarJOB_HISTORY
     */
    public Integer eliminarJobHistory(Integer employeeID, Date startDate) throws ExcepcionHR {
        conectarBD();
        int registrosAfectados = 0;
        String dml = "delete from JOB_HISTORY where EMPLOYEE_ID = ? and START_DATE = ?";

        try {
            PreparedStatement sentenciaPreparada = conexion.prepareCall(dml);
            sentenciaPreparada.setInt(1, employeeID);
            sentenciaPreparada.setDate(2, startDate);

            registrosAfectados = sentenciaPreparada.executeUpdate();
            sentenciaPreparada.close();
            conexion.close();

        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dml);
            e.setMensajeErrorUsuario("ERROR GENERAL DEL SISTEMA. Consulte con el administrador.");

            throw e;

        }
        return registrosAfectados;

    }

    /*
        leerTodosLosJobHistory
     */
    public ArrayList<JobHistory> leerTodosLosJobHistory() throws ExcepcionHR {
        conectarBD();
        ArrayList<JobHistory> listaJobHistory = new ArrayList<>();
        String dql = "select "
                + "JH.EMPLOYEE_ID, JH.START_DATE, JH.END_DATE, JH.JOB_ID, JH.DEPARTMENT_ID, "
                + "E.FIRST_NAME, E.LAST_NAME, E.EMAIL, "
                + "J.JOB_TITLE, "
                + "D.DEPARTMENT_NAME "
                + "from JOB_HISTORY JH "
                + "left join EMPLOYEES E on JH.EMPLOYEE_ID = E.EMPLOYEE_ID "
                + "left join JOBS J on JH.JOB_ID = J.JOB_ID "
                + "left join DEPARTMENTS D on JH.DEPARTMENT_ID = D.DEPARTMENT_ID "
                + "order by JH.EMPLOYEE_ID, JH.START_DATE";

        try {
            Statement sentencia = conexion.createStatement();
            ResultSet resultado = sentencia.executeQuery(dql);

            while (resultado.next()) {
                // Creamos el objeto Employee
                Employee empleado = new Employee();
                empleado.setEmployeeId(resultado.getInt("EMPLOYEE_ID"));
                empleado.setFirstName(resultado.getString("FIRST_NAME"));
                empleado.setLastName(resultado.getString("LAST_NAME"));
                empleado.setEmail(resultado.getString("EMAIL"));

                // Creación del objeto Job
                Job job = new Job();
                job.setJobId(resultado.getString("JOB_ID"));
                job.setJobTitle(resultado.getString("JOB_TITLE"));

                // Creación del objeto Department (puede ser NULL)
                Department departamento = null;
                Integer deptId = resultado.getInt("DEPARTMENT_ID");
                if (!resultado.wasNull()) {
                    departamento = new Department();
                    departamento.setDepartmentId(deptId);
                    departamento.setDepartmentName(resultado.getString("DEPARTMENT_NAME"));

                }

                // Creación y configuración de JobHistory
                JobHistory jobHistory = new JobHistory(empleado, resultado.getDate("START_DATE"), resultado.getDate("END_DATE"), job, departamento);
                listaJobHistory.add(jobHistory);

            }

            resultado.close();
            sentencia.close();
            conexion.close();

        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dql);
            e.setMensajeErrorUsuario("Error al leer el historial de trabajos. Consulte con el administrador");
            throw e;
        }
        return listaJobHistory;
    }

    /**
     *
     * @param locationId Int que servirá para buscar
     * @return Devuelve la localización si existe su identificador, si no
     * encuentra ninguna localización, devuleve null.
     * @throws ExcepcionHR Se lanzará cuando se produzca una excepción SQL.
     */
    public Location leerLocation(int locationId) throws ExcepcionHR {
        conectarBD();
        String dql = "select * from REGIONS R, COUNTRIES C, LOCATIONS L "
                + "where R.REGION_ID=C.REGION_ID and C.COUNTRY_ID=L.COUNTRY_ID and L.LOCATION_ID=" + locationId;
        Location l = null;
        Country c = null;
        Region r = null;
        try {
            Statement sentencia = conexion.createStatement();
            ResultSet resultado = sentencia.executeQuery(dql);

            if (resultado.next()) {
                l = new Location();
                l.setLocationId(resultado.getInt("LOCATION_ID"));
                l.setStreetAdress(resultado.getString("STREET_ADDRESS"));
                l.setPostalCode(resultado.getString("POSTAL_CODE"));
                l.setCity(resultado.getString("CITY"));
                l.setStateProvince(resultado.getString("STATE_PROVINCE"));

                c = new Country();
                c.setCountryId(resultado.getString("COUNTRY_ID"));
                c.setCountryName(resultado.getString("COUNTRY_NAME"));

                r = new Region();
                r.setRegionId(resultado.getInt("REGION_ID"));
                r.setRegionName(resultado.getString("REGION_NAME"));
                c.setRegion(r);
                l.setCountry(c);
            }

            sentencia.close();
            conexion.close();

        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dql);
            e.setMensajeErrorUsuario("Error general del sistema. Consulte con el admiinistrador ");
            throw e;
        }
        return l;
    }

    /**
     *
     * @param locationId Identificador de la tupla que se modificará
     * @param location Objeto de tipo "Location" con los datos que reemplazarán
     * al antiguo
     * @return Integer con la cantidad de registros actualizados
     * @throws ExcepcionHR Se lanzará cuando se produzca una excepción SQL.
     */
    public Integer modificarLocation(int locationId, Location location) throws ExcepcionHR {
        conectarBD();
        int registrosAfectados = 0;
        String procedimiento = "call MODIFICAR_LOCATION(?,?,?,?,?,?)";

        try {
            CallableStatement sentenciaLlamable = conexion.prepareCall(procedimiento);

            sentenciaLlamable.setObject(1, locationId, Type.INT);
            sentenciaLlamable.setString(2, location.getStreetAdress());
            sentenciaLlamable.setString(3, location.getPostalCode());
            sentenciaLlamable.setString(4, location.getCity());
            sentenciaLlamable.setString(5, location.getStateProvince());
            sentenciaLlamable.setObject(6, location.getCountry().getCountryId());
            registrosAfectados = sentenciaLlamable.executeUpdate();
            sentenciaLlamable.close();
            conexion.close();

        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(procedimiento);
            switch (ex.getErrorCode()) {
                case 2291:
                    e.setMensajeErrorUsuario("El país asociado a esta localización no existe");
                    break;
                case 1407:
                    e.setMensajeErrorUsuario("Es obligatorio que una localización tenga una ciudad.");
                    break;
                default:
                    e.setMensajeErrorUsuario("Error general del sistema. Consulte con el admiinistrador ");
            }
            throw e;
        }
        return registrosAfectados;
    }

    public ArrayList<Region> leerRegions() throws ExcepcionHR {
        conectarBD();
        ArrayList<Region> listaRegions = new ArrayList<>();

        String dql = "SELECT * FROM REGIONS";

        try {
            Statement sentencia = conexion.createStatement();
            ResultSet resultado = sentencia.executeQuery(dql);

            while (resultado.next()) {
                Region r = new Region();
                r.setRegionId(resultado.getInt("REGION_ID"));
                r.setRegionName(resultado.getString("REGION_NAME"));

                listaRegions.add(r);
            }

            resultado.close();
            sentencia.close();
            conexion.close();

        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dql);
            e.setMensajeErrorUsuario("Error al leer las regiones");
            throw e;
        }
        return listaRegions;
    }

    public Integer insertarRegion(Region region) throws ExcepcionHR {
        conectarBD();
        int registrosAfectados = 0;
        String dmlOracle = "insert into REGIONS(REGION_ID, REGION_NAME) values (SECUENCIA_REGION_ID.nextval, ?)";
        //String dmlMYSQL = "insert into REGIONS(REGION_NAME) values (?)";
        try {
            PreparedStatement sentenciaPreparada = conexion.prepareStatement(dmlOracle);
            sentenciaPreparada.setString(1, region.getRegionName());

            registrosAfectados = sentenciaPreparada.executeUpdate();

            sentenciaPreparada.close();
            conexion.close();

        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dmlOracle);
            e.setMensajeErrorUsuario("Error general del sistema. Consulte con el administrador");

            throw e;
        }

        return registrosAfectados;
    }

    public Integer modificarEmpleado(Employee emp) throws ExcepcionHR {
        conectarBD();
        int registrosAfectados;

        String sql = "call UPDATE_EMPLOYEE(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try {
            CallableStatement cs = conexion.prepareCall(sql);

            cs.setInt(1, emp.getEmployeeId());
            cs.setString(2, emp.getFirstName());
            cs.setString(3, emp.getLastName());
            cs.setString(4, emp.getEmail());
            cs.setString(5, emp.getPhoneNumber());
            cs.setObject(6, emp.getHireDate(), java.sql.Types.DATE);
            cs.setObject(7, emp.getJobId());
            cs.setObject(8, emp.getSalary(), java.sql.Types.NUMERIC);
            cs.setObject(9, emp.getCommissionPCT(), java.sql.Types.NUMERIC);
            cs.setObject(10, emp.getManager() != null ? emp.getManager().getEmployeeId() : null, java.sql.Types.NUMERIC);
            cs.setObject(11, emp.getDepartmentId() != null ? emp.getDepartmentId().getDepartmentId() : null, java.sql.Types.NUMERIC);

            registrosAfectados = cs.executeUpdate();

            cs.close();
            conexion.close();
        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(sql);

            switch (ex.getErrorCode()) {
                case 1407:
                    e.setMensajeErrorUsuario("Campos obligatorios no informados: fecha de contratación, email, trabajo y apellido");
                    break;
                case 2291:
                    e.setMensajeErrorUsuario("Ese trabajo, manager o departamento no existen");
                    break;
                case 2290:
                    e.setMensajeErrorUsuario("El salario debe ser mayor que 0");
                    break;
                case 1:
                    e.setMensajeErrorUsuario("El email ya existe");
                    break;
                default:
                    e.setMensajeErrorUsuario(
                            "Error general del sistema. Consulte con el administrador"
                    );
            }
            throw e;
        }

        return registrosAfectados;
    }

    public Employee leerEmpleado(Integer employeeId) throws ExcepcionHR {

        conectarBD();
        Employee e = null;

        String dql
                = "SELECT E.EMPLOYEE_ID, E.FIRST_NAME, E.LAST_NAME, E.EMAIL, E.PHONE_NUMBER, "
                + "E.HIRE_DATE, E.SALARY, E.COMMISSION_PCT, E.MANAGER_ID AS EMP_MANAGER_ID, "
                + "J.JOB_ID, J.JOB_TITLE, J.MIN_SALARY, J.MAX_SALARY, "
                + "D.DEPARTMENT_ID, D.DEPARTMENT_NAME, D.MANAGER_ID AS DEPT_MANAGER_ID, "
                + "L.LOCATION_ID, L.STREET_ADDRESS, L.POSTAL_CODE, L.CITY, L.STATE_PROVINCE, "
                + "C.COUNTRY_ID, C.COUNTRY_NAME, "
                + "R.REGION_ID, R.REGION_NAME "
                + "FROM EMPLOYEES E "
                + "JOIN JOBS J ON E.JOB_ID = J.JOB_ID "
                + "JOIN DEPARTMENTS D ON E.DEPARTMENT_ID = D.DEPARTMENT_ID "
                + "LEFT JOIN EMPLOYEES MGR_EMP ON E.MANAGER_ID = MGR_EMP.EMPLOYEE_ID "
                + "LEFT JOIN EMPLOYEES MGR_DEPT ON D.MANAGER_ID = MGR_DEPT.EMPLOYEE_ID "
                + "JOIN LOCATIONS L ON D.LOCATION_ID = L.LOCATION_ID "
                + "JOIN COUNTRIES C ON L.COUNTRY_ID = C.COUNTRY_ID "
                + "JOIN REGIONS R ON C.REGION_ID = R.REGION_ID "
                + "WHERE E.EMPLOYEE_ID = " + employeeId;

        try {
            Statement sentencia = conexion.createStatement();
            ResultSet resultado = sentencia.executeQuery(dql);

            if (resultado.next()) {
                e = new Employee();

                e.setEmployeeId(resultado.getInt("EMPLOYEE_ID"));
                e.setFirstName(resultado.getString("FIRST_NAME"));
                e.setLastName(resultado.getString("LAST_NAME"));
                e.setEmail(resultado.getString("EMAIL"));
                e.setPhoneNumber(resultado.getString("PHONE_NUMBER"));
                e.setHireDate(resultado.getDate("HIRE_DATE"));
                e.setSalary(resultado.getFloat("SALARY"));
                e.setCommissionPCT(resultado.getFloat("COMMISSION_PCT"));

                Job j = new Job();
                j.setJobId(resultado.getString("JOB_ID"));
                j.setJobTitle(resultado.getString("JOB_TITLE"));
                j.setMinSalary(resultado.getInt("MIN_SALARY"));
                j.setMaxSalary(resultado.getInt("MAX_SALARY"));
                e.setJobId(j);

                Department d = new Department();
                d.setDepartmentId(resultado.getInt("DEPARTMENT_ID"));
                d.setDepartmentName(resultado.getString("DEPARTMENT_NAME"));

                int deptManagerId = resultado.getInt("DEPT_MANAGER_ID");
                if (!resultado.wasNull() && deptManagerId != employeeId) {
                    d.setManager(leerEmpleado(deptManagerId));
                }

                Location l = new Location();
                l.setLocationId(resultado.getInt("LOCATION_ID"));
                l.setStreetAdress(resultado.getString("STREET_ADDRESS"));
                l.setPostalCode(resultado.getString("POSTAL_CODE"));
                l.setCity(resultado.getString("CITY"));
                l.setStateProvince(resultado.getString("STATE_PROVINCE"));

                Country c = new Country();
                c.setCountryId(resultado.getString("COUNTRY_ID"));
                c.setCountryName(resultado.getString("COUNTRY_NAME"));

                Region r = new Region();
                r.setRegionId(resultado.getInt("REGION_ID"));
                r.setRegionName(resultado.getString("REGION_NAME"));
                c.setRegion(r);

                l.setCountry(c);
                d.setLocation(l);
                e.setDepartmentId(d);

                int managerId = resultado.getInt("EMP_MANAGER_ID");
                if (!resultado.wasNull() && managerId != employeeId) {
                    e.setManager(leerEmpleado(managerId));
                }
            }

            sentencia.close();
            conexion.close();
        } catch (SQLException ex) {
            ExcepcionHR exHR = new ExcepcionHR();
            exHR.setCodigoErrorBD(ex.getErrorCode());
            exHR.setMensajeErrorBD(ex.getMessage());
            exHR.setSentenciaSQL(dql);
            exHR.setMensajeErrorUsuario("Error general del sistema. Consulte con el administrador");
            throw exHR;
        }

        return e;
    }

    //--------------------------ELIMINAR EMPLOYEES--------------------------------
    /**
     * Elimina un único registro de la tabla Employees
     *
     * @param employeeId Identificador del empleado que se desea eliminar
     * @return Cantidad de registros eliminados
     * @throws ExcepcionHR Se lanzará cuando se produzca un error de base de
     * datos
     * @author Marina Leonardo Romero
     * @version 1.0
     * @since AaD 1.0
     */
    public Integer eliminarEmployee(Integer employeeId) throws ExcepcionHR {
        conectarBD();
        int registrosAfectados = 0;
        String dml = "delete EMPLOYEES where EMPLOYEE_ID = " + employeeId;

        try {
            Statement sentencia = conexion.createStatement();
            registrosAfectados = sentencia.executeUpdate(dml);

            sentencia.close();
            conexion.close();

        } catch (SQLException ex) {
            ExcepcionHR e = new ExcepcionHR();
            e.setCodigoErrorBD(ex.getErrorCode());
            e.setMensajeErrorBD(ex.getMessage());
            e.setSentenciaSQL(dml);

            switch (ex.getErrorCode()) {
                case 2292: //FK
                    e.setMensajeErrorUsuario("No se puede eliminar el empleado por referencias en el historial, es el manager de un departamento o el jefe de algun empleado.");
                    break;
                default:
                    e.setMensajeErrorUsuario("Error general del sistema. Consulte con el administrador");
            }
            throw e;
        }
        return registrosAfectados;
    }

    //--------------------------LEER TODOS EMPLOYEES--------------------------------
    /**
     * Devuelve un listado de todos los empleados
     *
     * @return ArrayList de empleados
     * @throws ExcepcionHR Se lanzará cuando se produzca un error de base de
     * datos
     * @author Marina Leonardo Romero
     * @version 1.0
     * @since AaD 1.0
     */
    public ArrayList<Employee> leerEmployees() throws ExcepcionHR {

        conectarBD();
        ArrayList<Employee> listaEmployees = new ArrayList<>();

        String dql
                = "SELECT E.*, D.DEPARTMENT_NAME, J.JOB_TITLE, "
                + "M.EMPLOYEE_ID AS M_EMPLOYEE_ID, "
                + "M.FIRST_NAME AS M_FIRST_NAME, "
                + "M.LAST_NAME AS M_LAST_NAME, "
                + "M.EMAIL AS M_EMAIL, "
                + "M.PHONE_NUMBER AS M_PHONE_NUMBER, "
                + "M.HIRE_DATE AS M_HIRE_DATE, "
                + "M.SALARY AS M_SALARY, "
                + "M.COMMISSION_PCT AS M_COMMISSION_PCT "
                + "FROM EMPLOYEES E, DEPARTMENTS D, JOBS J, EMPLOYEES M "
                + "WHERE E.DEPARTMENT_ID = D.DEPARTMENT_ID "
                + "AND E.JOB_ID = J.JOB_ID "
                + "AND E.MANAGER_ID = M.EMPLOYEE_ID";

        try {
            Statement sentencia = conexion.createStatement();
            ResultSet resultado = sentencia.executeQuery(dql);

            while (resultado.next()) {

                Employee e = new Employee();
                e.setEmployeeId(resultado.getInt("EMPLOYEE_ID"));
                e.setFirstName(resultado.getString("FIRST_NAME"));
                e.setLastName(resultado.getString("LAST_NAME"));
                e.setEmail(resultado.getString("EMAIL"));
                e.setPhoneNumber(resultado.getString("PHONE_NUMBER"));
                e.setHireDate(resultado.getDate("HIRE_DATE"));
                e.setSalary(resultado.getFloat("SALARY"));
                e.setCommissionPCT(resultado.getFloat("COMMISSION_PCT"));

                Department d = new Department();
                d.setDepartmentId(resultado.getInt("DEPARTMENT_ID"));
                d.setDepartmentName(resultado.getString("DEPARTMENT_NAME"));
                e.setDepartmentId(d);

                Job j = new Job();
                j.setJobId(resultado.getString("JOB_ID"));
                j.setJobTitle(resultado.getString("JOB_TITLE"));
                e.setJobId(j);

                if (resultado.getObject("M_EMPLOYEE_ID") != null) {
                    Employee m = new Employee();
                    m.setEmployeeId(resultado.getInt("M_EMPLOYEE_ID"));
                    m.setFirstName(resultado.getString("M_FIRST_NAME"));
                    m.setLastName(resultado.getString("M_LAST_NAME"));
                    m.setEmail(resultado.getString("M_EMAIL"));
                    m.setPhoneNumber(resultado.getString("M_PHONE_NUMBER"));
                    m.setHireDate(resultado.getDate("M_HIRE_DATE"));
                    m.setSalary(resultado.getFloat("M_SALARY"));
                    m.setCommissionPCT(resultado.getFloat("M_COMMISSION_PCT"));

                    e.setManager(m);
                }

                listaEmployees.add(e);
            }

            resultado.close();
            sentencia.close();
            conexion.close();

        } catch (SQLException ex) {
            ExcepcionHR exHR = new ExcepcionHR();
            exHR.setCodigoErrorBD(ex.getErrorCode());
            exHR.setMensajeErrorBD(ex.getMessage());
            exHR.setSentenciaSQL(dql);
            exHR.setMensajeErrorUsuario(
                    "Error general del sistema. Consulte con el administrador");
            throw exHR;
        }

        return listaEmployees;
    }
}
