package ifxexec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;

public class BdUtils {
	
	public static Connection conecta(String banco) throws Exception {
		
		String informixDir = System.getenv("INFORMIXDIR");
		if (informixDir == null) {
			throw new Exception("INFORMIXDIR nao definida");
		}
		
		String informixServer = System.getenv("INFORMIXSERVER");
		if (informixServer == null) {
			throw new Exception("INFORMIXSERVER nao definida");
		}
		
		String informixHosts = System.getenv("INFORMIXSQLHOSTS");
		if (informixHosts == null) {
			informixHosts = informixDir + "/etc/sqlhosts";
		}
		
		String host = null;
        String servico = null;
        BufferedReader sqlhosts = new BufferedReader(new FileReader(informixHosts));
        String linha;
        while ((linha = sqlhosts.readLine()) != null) {
            if (linha.startsWith(informixServer)) {
                String[] partes = linha.split("\\s+");
                host = partes[2];
                servico = partes[3];
                break;
            }
        }
        sqlhosts.close();
        if (host == null) {
        	throw new Exception("Configuracao nao encontrada no sqlhosts");
        }
		
        String porta = null;
        BufferedReader services = new BufferedReader(new FileReader("/etc/services"));
        while ((linha = services.readLine()) != null) {
            if (linha.startsWith(servico)) {
                String[] partes = linha.split("\\s+");
                porta = partes[1].split("/")[0];
                break;
            }
        }
        services.close();
        if (porta == null) {
        	throw new Exception("Porta nao encontrada: " + servico);
        }        
        
        String driver = "com.informix.jdbc.IfxDriver";
        Class.forName(driver);

        String stringConexao = String.format(
                "jdbc:informix-sqli://%s:%s/%s:informixserver=%s",
                host, porta, banco, informixServer);
        System.err.println(stringConexao);
        String user = System.getenv("INFORMIX_USER");
        String passwd = System.getenv("INFORMIX_PASSWD");
        Connection conexao = null;
        if (user != null) {
        	conexao = DriverManager.getConnection(stringConexao, user, passwd);
        } else {
        	conexao = DriverManager.getConnection(stringConexao);
        }
        return conexao;
		
	}

}
