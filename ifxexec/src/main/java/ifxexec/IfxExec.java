package ifxexec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class IfxExec {
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Uso: java IfxExec <banco> <arquivo.sql>");
			return;
		}
		String banco = args[0];
		String arquivo = args[1];
		
		Connection conexao = BdUtils.conecta(banco);
		
		StringBuilder sb = new StringBuilder();
		BufferedReader reader = new BufferedReader(new FileReader(arquivo));
        String linha;
        while ((linha = reader.readLine()) != null) {
        	sb.append(linha + "\n");
        }		
		reader.close();
		String comando = sb.toString();
		
		Statement stmt = conexao.createStatement();
		
		PrintStream unload = null;
		
		if (comando.toLowerCase().startsWith("unload")) {
			int p = comando.toLowerCase().indexOf("select");
			if (p > 0) {
				String cmdUnload = comando.substring(0, p);
				String[] partes = cmdUnload.split("\\s+");
				if (partes.length == 3) {
					unload = new PrintStream(partes[2]);
				}
				comando = comando.substring(p);
			}
		}
		
		if (comando.toLowerCase().startsWith("select")) {
			
			ResultSet cursor = stmt.executeQuery(comando);
			int numColunas = cursor.getMetaData().getColumnCount();
			
			if (unload == null) {
				for (int c=0; c<numColunas; c++) {
					int tam = cursor.getMetaData().getColumnDisplaySize(c+1);
					String nome = cursor.getMetaData().getColumnName(c+1);
					if (nome.length() > tam) {
						tam = nome.length();
					}
					String fmt = String.format("%%-%ds", tam+1);
					System.out.printf(fmt, nome);
				}
				System.out.println();
				System.out.println();
			}
			
			while (cursor.next()) {
				
				for (int c=0; c<numColunas; c++) {
					String nome = cursor.getMetaData().getColumnName(c+1);
					String tipo = cursor.getMetaData().getColumnTypeName(c+1);
					int tam = cursor.getMetaData().getColumnDisplaySize(c+1);
					if (nome.length() > tam) {
						tam = nome.length();
					}					
					int precisao = cursor.getMetaData().getPrecision(c+1);
					int escala = cursor.getMetaData().getScale(c+1);
				
					String fmt = null;
					
					switch (tipo) {
				
					case "char":
					case "varchar":
					case "text":
						String s = cursor.getString(c+1);
						if (cursor.wasNull() || (s == null)) {
							s = "";
						}
						fmt = String.format("%%-%ds", tam+1);
						if (unload == null) {
							System.out.printf(fmt, s);
						} else {
							unload.printf("%s|", s.trim());
						}
						break;
					
					case "int":
					case "smallint":
					case "serial":
						int i = cursor.getInt(c+1);
						if (cursor.wasNull()) {
							i = 0;
						}						
						fmt = String.format("%%%dd", precisao);
						s = String.format(fmt, i);
						fmt = String.format("%%%ds ", tam);
						if (unload == null) {
							System.out.printf(fmt, s);
						} else {
							unload.printf("%s|", s.trim());
						}
						break;
						
					case "float":
						float f = cursor.getFloat(c+1);
						if (cursor.wasNull()) {
							f = 0f;
						}						
						fmt = String.format("%%%d.%df", precisao, escala);
						s = String.format(fmt, f);
						fmt = String.format("%%%ds ", tam);
						if (unload == null) {
							System.out.printf(fmt, s);
						} else {
							unload.printf("%s|", s.trim());
						}
						break;
						
					case "double":
						double d = cursor.getDouble(c+1);
						if (cursor.wasNull()) {
							d = 0.;
						}						
						fmt = String.format("%%%d.%df", precisao, escala);
						s = String.format(fmt, d);
						fmt = String.format("%%%ds ", tam);
						if (unload == null) {
							System.out.printf(fmt, s);
						} else {
							unload.printf("%s|", s.trim());
						}
						break;
						
					case "date":
						DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
						java.sql.Date dSql = cursor.getDate(c+1);
						Date dt = dSql != null ? new Date(dSql.getTime()) : null;
						s = dt != null ? df.format(dt) : "";
						fmt = String.format("%%-%ds", tam+1);
						if (unload == null) {
							System.out.printf(fmt, s);
						} else {
							unload.printf("%s|", s.trim());
						}
						break;
						
					default:
						if (tipo.startsWith("datetime")) {
							df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
							dSql = cursor.getDate(c+1);
							dt = dSql != null ? new Date(dSql.getTime()) : null;
							s = dt != null ? df.format(dt) : "";
							fmt = String.format("%%-%ds", tam+1);
							if (unload == null) {
								System.out.printf(fmt, s);
							} else {
								unload.printf("%s|", s.trim());
							}
							break;
						}
						System.out.print(nome + ":" + tipo);
						break;
					
					}
					
				}
				if (unload == null) {
					System.out.println();
				} else {
					unload.println();
				}
			
			}
			
			cursor.close();
		} else {
			
			// comando de atualizacao
			stmt.execute(comando);
			
		}
		
		stmt.close();
        conexao.close();
        if (unload != null) {
        	unload.close();
        }
		
	}

}
