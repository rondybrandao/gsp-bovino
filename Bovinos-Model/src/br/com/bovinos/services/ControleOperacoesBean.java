/**
 * 
 */
package br.com.bovinos.services;

import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.sql.ResultSet;

import javax.ejb.EJBException;
import javax.ejb.SessionBean;
import javax.ejb.SessionContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sankhya.util.JdbcUtils;
import com.sankhya.util.JsonUtils;
import com.sankhya.util.StringUtils;
import com.sankhya.util.XMLUtils;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.util.BaseSPBean;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.SPBeanUtils;
import br.com.sankhya.ws.ServiceContext;

/**
 * @author Murillo Henrique S Soares
 * @ejb.bean name="ControleOperacoesSP" 
 * jndi-name="br/com/bovinos/services/ControleOperacoesSP" 
 * type="Stateless" 
 * transaction-type="Container" 
 * view-type="remote"
 * @ejb.transaction type="Supports" *
 * @ejb.util generate="false"
 */
public class ControleOperacoesBean extends BaseSPBean implements SessionBean {

	private static String		RESPOSTA	= "resposta";
	private ServiceContext		ctx			= null;

	protected SessionContext	context;
	private SessionHandle		hnd			= null;
	private JdbcWrapper			jdbc		= null;
	private Boolean				isOpen		= false;
	private NativeSql			nativeSql	= null;
	int							p_validacao;
	int							p_validacaoRetorno;

	@Override
	protected void setupContext(ServiceContext ctx) throws Exception {
		SPBeanUtils.setupContext(ctx);
	}

	@Override
	protected void throwExceptionRollingBack(Throwable e) throws MGEModelException {
		SPBeanUtils.throwExceptionRollingBack(e, context);
	}

	@Override
	public void setSessionContext(SessionContext ctx) throws EJBException, RemoteException {
		this.context = ctx;
	}

	@Override
	public void ejbActivate() throws EJBException, RemoteException {
	}

	@Override
	public void ejbPassivate() throws EJBException, RemoteException {
	}

	@Override
	public void ejbRemove() throws EJBException, RemoteException {
	}
	
	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void registraItemConf(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		String NROBRINCO = JsonUtils.getString(requestBody, "NROBRINCO");
		if (NROBRINCO == null)
			throw new Exception("Não foi informado o NROBRINCO, Entre em contato com a TI.");
		String NROCONF = JsonUtils.getString(requestBody, "NROCONF");
		if (NROCONF == null)
			throw new Exception("Não foi informado o NROCONF, Entre em contato com a TI.");
		BigDecimal PESOATUAL = JsonUtils.getBigDecimalOrZero(requestBody, "PESOATUAL");
		String OBS = StringUtils.getEmptyAsNull(JsonUtils.getString(requestBody, "OBS"));

		Boolean success = false;
		Boolean validations = false;
		ResultSet rset = null;
		String msg_erro = "";
		try {
			openSession();

			nativeSql.resetSqlBuf();
			nativeSql.setNamedParameter("NROBRINCO", NROBRINCO);
			nativeSql.setNamedParameter("NROCONF", NROCONF);
			nativeSql.setNamedParameter("PESOATUAL", PESOATUAL);
			nativeSql.setNamedParameter("OBS", OBS);
			
			validations = brincoCheck("SELECT 1 FROM BVBRINCO WHERE NUBRINCO = :NROBRINCO");
			
			if(!validations) {
				msg_erro = "Brinco não cadastrado ou não vinculado à um animal, verifique!";
			}
			
			validations = brincoCheck("SELECT 1 FROM BVBRINCO WHERE NUBRINCO = :NROBRINCO AND ATIVO = 'S'");
			
			if(!validations) {
				msg_erro = "Brinco não está ativo, verifique!";
			}
			
			validations = brincoCheck("SELECT 1 FROM BVBRINCO WHERE NUBRINCO = :NROBRINCO AND ATIVO = 'S' AND TO_CHAR(NVL(DTINATIVO, '01/01/1970'), 'DD/MM/YYYY') < TO_CHAR(SYSDATE, 'DD/MM/YYYY')");
			
			if(!validations) {
				msg_erro = "Brinco expirado, verifique!";
			}
			
			//Confere se já existe algum brinco conferido
			validations = brincoCheck("SELECT 1 FROM BVWFILACONFITE WHERE BOTON = :NROBRINCO AND NROCONF = :NROCONF");
			
			if(validations) {
				success = true;
			} else {
				nativeSql.resetSqlBuf();
				success = nativeSql.executeUpdate("MERGE INTO BVWFILACONFITE tgt USING (SELECT BOTON, :PESOATUAL AS PESOATUAL, SYSDATE AS DHCOLETA, :OBS AS OBS, :NROCONF AS NROCONF, IDANIMAL FROM BVWBRINCOS WHERE BOTON = :NROBRINCO) src ON (tgt.BOTON=src.BOTON AND tgt.NROCONF=src.NROCONF) WHEN NOT MATCHED THEN INSERT (BOTON, PESOATUAL, DHCOLETA, OBS, NROCONF, IDANIMAL) VALUES (src.BOTON, src.PESOATUAL, src.DHCOLETA, src.OBS, src.NROCONF, src.IDANIMAL)");
			}
			
			XMLUtils.addAttributeElement(ctx.getBodyElement(), "SUCCESS", success);
			XMLUtils.addAttributeElement(ctx.getBodyElement(), "MSG", msg_erro);

		} catch (Exception e) {
			throw new Exception("Erro buscadadosBrinco.", e);
		} finally {
			closeSession();
		}
	}

	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void validaBrincoJaConf(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		String NROBRINCO = JsonUtils.getString(requestBody, "NROBRINCO");
		if (NROBRINCO == null)
			throw new Exception("Não foi informado o NROBRINCO, Entre em contato com a TI.");

		String NROCONF = JsonUtils.getString(requestBody, "NROCONF");
		if (NROCONF == null)
			throw new Exception("Não foi informado o NROCONF, Entre em contato com a TI.");

		ResultSet rset = null;
		try {
			openSession();

			nativeSql.resetSqlBuf();
			nativeSql.setNamedParameter("NROCONF", NROCONF);
			nativeSql.setNamedParameter("NROBRINCO", NROBRINCO);
			rset = nativeSql.executeQuery("SELECT COUNT(1) AS RESULTADO FROM BVWFILACONFITE WHERE NROCONF = :NROCONF AND BOTON = :NROBRINCO");

			while (rset.next()) {
				XMLUtils.addAttributeElement(ctx.getBodyElement(), "RESULTADO", rset.getString("RESULTADO"));
			}

		} catch (Exception e) {
			throw new Exception("Erro validaBrincoJaConf.", e);
		} finally {
			closeSession();
			liberarRecursosConexaoBD(rset);
		}
	}

	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void alteraStatusConf(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		String NROCONF = JsonUtils.getString(requestBody, "NROCONF");
		if (NROCONF == null)
			throw new Exception("Nro Conferencia não informado, Entre em contato com a TI.");

		int TIPO = JsonUtils.getInt(requestBody, "TIPO");

		try {
			openSession();
			nativeSql.resetSqlBuf();
			nativeSql.setNamedParameter("NROCONF", NROCONF);

			if (TIPO == 1) {
				nativeSql.appendSql("UPDATE BVWFILACONFCB SET STATUS = 'P', DTFIM = NULL WHERE NROCONF = :NROCONF"); // MARCA COMO PENDENTE
			} else if (TIPO == 2) {
				nativeSql.appendSql("UPDATE BVWFILACONFCB SET STATUS = 'E', DTFIM = NULL WHERE NROCONF = :NROCONF"); // MARCA COMO EM ANDAMENTO
			} else if (TIPO == 3) {
				nativeSql.appendSql("UPDATE BVWFILACONFCB SET STATUS = 'F', DTFIM = SYSDATE WHERE NROCONF = :NROCONF"); // MARCA COMO FINALIZADO
			}
			nativeSql.executeUpdate();
		} catch (Exception e) {
			throw new Exception("Erro alteraStatusConf.", e);
		} finally {
			closeSession();
		}
	}

	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void registraPesoConferencia(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		String OBS = JsonUtils.getString(requestBody, "OBS");
		String IDANIMAL = JsonUtils.getString(requestBody, "IDANIMAL");
		String BOTON = JsonUtils.getString(requestBody, "BOTON");
		String NROCONF = JsonUtils.getString(requestBody, "NROCONF");
		String PESOATUAL = JsonUtils.getString(requestBody, "PESOATUAL");

		if (PESOATUAL == null)
			throw new Exception("O valor de PESO ATUAL  não informado, Entre em contato com a TI.");

		try {
			openSession();
			nativeSql.resetSqlBuf();
			nativeSql.setNamedParameter("IDANIMAL", IDANIMAL);
			nativeSql.setNamedParameter("BOTON", BOTON);
			nativeSql.setNamedParameter("PESOATUAL", PESOATUAL);
			nativeSql.setNamedParameter("OBS", OBS);
			nativeSql.setNamedParameter("NROCONF", NROCONF);
			nativeSql.appendSql("INSERT INTO BVWFILACONFITE (IDANIMAL,BOTON,PESOATUAL,OBS,NROCONF) VALUES (:IDANIMAL, :BOTON, :PESOATUAL, :OBS, :NROCONF)");
			nativeSql.executeUpdate();
		} catch (Exception e) {
			throw new Exception("Erro registraPesoConferencia.", e);
		} finally {
			closeSession();
		}
	}

	private int geraIDAnimal() throws Exception {

		int retorno = 0;
		ResultSet rset = null;
		try {
			openSession();
			nativeSql.resetSqlBuf();
			rset = nativeSql.executeQuery("SELECT MAX(IDANIMAL + 1) AS ID FROM BVANIMAIS");

			if (rset.next()) {
				retorno = rset.getInt("ID");
			}

		} catch (Exception e) {
			throw new Exception("Erro geraIDAnimal.", e);
		} finally {
			closeSession();
			liberarRecursosConexaoBD(rset);
		}

		return retorno;
	}

	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void registraCadNovoConferencia(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		String OBS = JsonUtils.getString(requestBody, "OBS");
		String PESOATUAL = JsonUtils.getString(requestBody, "PESOATUAL");
		String NOMEANIMAL = JsonUtils.getString(requestBody, "NOMEANIMAL");
		String PELAGEM = JsonUtils.getString(requestBody, "PELAGEM");
		String NROCONF = JsonUtils.getString(requestBody, "NROCONF");
		String SEXO = JsonUtils.getString(requestBody, "SEXO");

		try {

			int IDANIMAL = geraIDAnimal();

			openSession();
			nativeSql.resetSqlBuf();

			nativeSql.resetSqlBuf();
			nativeSql.setNamedParameter("IDANIMAL", IDANIMAL);
			nativeSql.setNamedParameter("NOMEANIMAL", NOMEANIMAL);
			nativeSql.setNamedParameter("SEXO", SEXO);
			nativeSql.setNamedParameter("PELAGEM", PELAGEM);
			nativeSql.appendSql("INSERT INTO BVANIMAIS (IDANIMAL,NOME,BOTON,SEXO,PELAGEM,STATUS,DTCAD,DHALTER,DTNASC) VALUES(:IDANIMAL, :NOMEANIMAL, 'C', :IDANIMAL, :SEXO, :PELAGEM,'S', SYSDATE, SYSDATE, SYSDATE)");
			nativeSql.executeUpdate();

			nativeSql.resetSqlBuf();
			nativeSql.setNamedParameter("IDANIMAL", IDANIMAL);
			nativeSql.setNamedParameter("PESOATUAL", PESOATUAL);
			nativeSql.setNamedParameter("OBS", OBS);
			nativeSql.setNamedParameter("NROCONF", NROCONF);
			nativeSql.appendSql("INSERT INTO BVWFILACONFITE (IDANIMAL,BOTON,PESOATUAL,OBS,NROCONF) VALUES(:IDANIMAL, 'C', :IDANIMAL, :PESOATUAL, :OBS, :NROCONF)");
			nativeSql.executeUpdate();
		} catch (Exception e) {
			throw new Exception("Erro registraCadNovoConferencia.", e);
		} finally {
			closeSession();
		}

	}

	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void registraAnimalRat(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		final BigDecimal IDRATFINBOV = JsonUtils.getBigDecimal(requestBody, "IDRATFINBOV");
		final JsonArray IDANIMALIST = JsonUtils.getJsonArray(requestBody, "IDANIMALIST");
		final JsonArray NUFINLIST = JsonUtils.getJsonArray(requestBody, "NUFINLIST");

		try {
			openSession();
			
			nativeSql.resetSqlBuf();
			nativeSql.appendSql("UPDATE BVANIMAIS SET IDRATFINBOV = :IDRATFINBOV WHERE IDANIMAL = :IDANIMAL");
			nativeSql.setNamedParameter("IDRATFINBOV", IDRATFINBOV);
			
			for(int i=0;i<IDANIMALIST.size();i++) {
				nativeSql.setNamedParameter("IDANIMAL", IDANIMALIST.get(i).getAsBigDecimal());
				nativeSql.executeUpdate();
			}
			
			nativeSql.resetSqlBuf();
			nativeSql.appendSql("UPDATE TGFFIN SET IDRATFINBOV = :IDRATFINBOV WHERE NUFIN = :NUFIN");
			
			for(int j=0;j<NUFINLIST.size();j++) {
				nativeSql.setNamedParameter("NUFIN", NUFINLIST.get(j).getAsBigDecimal());
				nativeSql.executeUpdate();
			}
			
			nativeSql.resetSqlBuf();
			nativeSql.appendSql("INSERT INTO BVRATFIN (IDRATEIO, DTINCLUSAO, DTALTER, CODUSUALTER, CODUSUINC, IDANIMAL, NUFIN)  VALUES(:IDRATFINBOV, SYSDATE, SYSDATE, STP_GET_CODUSULOGADO, STP_GET_CODUSULOGADO, :IDANIMAL, :NUFIN)");
			
			for(int i=0;i<IDANIMALIST.size();i++) {
				for(int j=0;j<NUFINLIST.size();j++) {
					nativeSql.setNamedParameter("IDANIMAL", IDANIMALIST.get(i).getAsBigDecimal());
					nativeSql.setNamedParameter("NUFIN", NUFINLIST.get(j).getAsBigDecimal());
					nativeSql.executeUpdate();
				}
			}
			
		} catch (Exception e) {
			throw new Exception("Erro registraAnimalRat. BVRATFIN. IDRATFINBOV = " + IDRATFINBOV + " , IDANIMALIST = " + IDANIMALIST.toString() + " , NUFINLIST = " + NUFINLIST.toString() + ".\nErr: " + e);
		} finally {
			closeSession();
		}
	}

	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void geraTGFNUM(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		String TABELA = JsonUtils.getString(requestBody, "TABELA");
		if (TABELA == null)
			throw new Exception("Não foi informado a TABELA, Entre em contato com a TI.");

		String PK = JsonUtils.getString(requestBody, "PK");
		if (PK == null)
			throw new Exception("Não foi informado a PK, Entre em contato com a TI.");
		
		String value = AD_GET_PROX_NUM_DISP(TABELA, PK);
				
		if(value != null) {
			XMLUtils.addAttributeElement(ctx.getBodyElement(), "RESULTADO", value);
		}
	}
	
	private String AD_GET_PROX_NUM_DISP(String TABELA, String PK) throws Exception {
		ResultSet rset = null;
		String value = null;
		try {
			openSession();
			nativeSql.resetSqlBuf();
			nativeSql.setNamedParameter("TABELA", TABELA);
			nativeSql.setNamedParameter("PK", PK);

			rset = nativeSql.executeQuery("SELECT AD_GET_PROX_NUM_DISP(:TABELA, :PK) AS RESULTADO FROM DUAL");

			if (rset.next()) {
				value = rset.getString("RESULTADO");
			}

		} catch (Exception e) {
			throw new Exception("Erro AD_GET_PROX_NUM_DISP.", e);
		} finally {
			closeSession();
			liberarRecursosConexaoBD(rset);
		}
		
		return value;
	}

	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void estornarRateioFinAnimal(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();
		String IDRATFINBOV = JsonUtils.getString(requestBody, "IDRATFINBOV");

		try {
			openSession();
			nativeSql.setNamedParameter("IDRATFINBOV", IDRATFINBOV);

			//DESFAZ FINANCEIRO
			nativeSql.resetSqlBuf();
			nativeSql.appendSql("UPDATE TGFFIN SET IDRATFINBOV = NULL WHERE IDRATFINBOV = :IDRATFINBOV");
			nativeSql.executeUpdate();

			//DESFAZ ANIMAL
			nativeSql.resetSqlBuf();
			nativeSql.appendSql("UPDATE BVANIMAIS SET IDRATFINBOV = NULL WHERE IDRATFINBOV = :IDRATFINBOV");
			nativeSql.executeUpdate();
			
			//DESFAZ RATEIO
			nativeSql.resetSqlBuf();
			nativeSql.appendSql("DELETE FROM BVRATFIN WHERE IDRATEIO = :IDRATFINBOV");
			nativeSql.executeUpdate();

		} catch (Exception e) {
			throw new Exception("Erro estornarRateioFinAnimal.", e);
		} finally {
			closeSession();
		}
	}
	
	private Boolean brincoCheck(final String query) throws Exception {
		nativeSql.resetSqlBuf();
		ResultSet rset = nativeSql.executeQuery(query);
		return rset.next();
	}

	private void openSession() throws Exception {
		try {
			if (isOpen) {
				return;
			}
			hnd = JapeSession.open();
			EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();

			jdbc = dwfFacade.getJdbcWrapper();
			jdbc.openSession();

			nativeSql = new NativeSql(jdbc);
			isOpen = true;
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Erro ao abrir sessão.", e);
		}
	}

	private void closeSession() throws Exception {
		try {
			if (isOpen) {
				NativeSql.releaseResources(nativeSql);
				JdbcWrapper.closeSession(jdbc);
				JapeSession.close(hnd);
				isOpen = false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Erro ao fechar sessão.", e);
		}
	}

	private void liberarRecursosConexaoBD(ResultSet rs) {
		//Limpando váriveis de conexão
		if (nativeSql != null)
			nativeSql.resetSqlBuf();
		if (rs != null)
			JdbcUtils.closeResultSet(rs);
	}
}
