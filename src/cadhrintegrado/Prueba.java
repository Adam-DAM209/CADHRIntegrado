/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cadhrintegrado;

import cadhrintegrado.CADHRIntegrado;
import pojoshr.ExcepcionHR;
import java.util.ArrayList;
import java.sql.Date;
import java.time.LocalDate;
import java.util.logging.Level;
import java.util.logging.Logger;
import pojoshr.*;

/**
 *
 * @author DAM209
 */
public class Prueba {

    public static void main(String[] args) {

//        int registrosAfectados = 0;
//        try {
//            Department dep = new Department();
//            Location loc = new Location();
//            Employee man = new Employee();
//            
//            man.setEmployeeId(100);
//            loc.setLocationId(2);
//            
//            dep.setDepartmentName(null);
//            dep.setLocation(loc);
//            dep.setManager(man);
//            
//            registrosAfectados = cad.modificarDepartment(20, dep);
//            System.out.println("Registros eliminados: " + registrosAfectados);
//        } catch (ExcepcionHR ex) {
//            System.out.println(ex);
//        }
        

/////////////////////////////////////

//            int registrosAfectados = 0;
//            try {
//                CADHR cad = new CADHR();
//                
//                
//                registrosAfectados = cad.eliminarCountry("AR");
//            } catch (ExcepcionHR e) {
//                System.out.println(e);
//            }
//            System.out.println("Registros afectados: " + registrosAfectados);

//          try {
//            CADHR cad = new CADHR();
//
//            Employee emp = new Employee();
//            emp.setEmployeeId(105); 
//            emp.setFirstName("Asier");
//            emp.setLastName("Gutiérrez");
//            emp.setEmail("AGO");
//            emp.setPhoneNumber("431.123.321");
//            emp.setHireDate(java.sql.Date.valueOf(LocalDate.of(2025, 12, 31)));
//            
//            Job job = new Job();
//            job.setJobId("IT_PROG");
//            emp.setJobId(job);
//            
//            emp.setSalary(-1f);
//            emp.setCommissionPCT(null); 
//            
//            Employee manager = new Employee();
//            manager.setEmployeeId(101);
//            emp.setManager(manager);
//            
//            Department dept = new Department();
//            dept.setDepartmentId(80);
//            emp.setDepartmentId(dept);
//            System.out.println(emp);
//            int registros = cad.modificarEmpleadoCallable(emp);
//
//            System.out.println("Registros afectados: " + registros);
//            System.out.println("Empleado modificado correctamente: " + emp);
//
//        } catch (ExcepcionHR ex) {
//            System.out.println(ex);
//       
//        }
  try {
            CADHRIntegrado cad = new CADHRIntegrado();
            
            Employee e = cad.leerEmpleado(100);
             System.out.println(e);

        } catch (ExcepcionHR ex) {
            System.out.println("Mensaje usuario: " + ex.getMensajeErrorUsuario());
            System.out.println("Mensaje BD: " + ex.getMensajeErrorBD());
        }


    }
}
