package br.com.bovinos.services;

import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.sql.ResultSet;

import javax.ejb.EJBException;
import javax.ejb.SessionBean;
import javax.ejb.SessionContext;

import org.json.JSONObject;

import com.google.gson.JsonObject;
import com.sankhya.util.JdbcUtils;
import com.sankhya.util.JsonUtils;
import com.sankhya.util.TimeUtils;
import com.sankhya.util.XMLUtils;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.jape.wrapper.fluid.FluidCreateVO;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.util.BaseSPBean;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.SPBeanUtils;
import br.com.sankhya.ws.ServiceContext;

/**
 * @author Murillo H S Soares
 * @ejb.bean name="BovinosOperacoesSP" 
 * jndi-name="br/com/bovinos/services/BovinosOperacoesSP" 
 * type="Stateless" 
 * transaction-type="Container" 
 * view-type="remote"
 * @ejb.transaction type="Supports" 
 *	
 * @ejb.util generate="false"
 */
public class BovinosOperacoesSPBean extends BaseSPBean implements SessionBean {

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
	public void implantar(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		BigDecimal IDCOBERTURA = JsonUtils.getBigDecimal(requestBody, "IDCOBERTURA");
		String STATUS = JsonUtils.getString(requestBody, "STATUS");
		String DTA = JsonUtils.getString(requestBody, "DTA");
		BigDecimal CODUSU = JsonUtils.getBigDecimal(requestBody, "CODUSU");

		try {
			openSession();

			nativeSql.resetSqlBuf();
			nativeSql.appendSql("UPDATE BVCOBERTURAS SET STATUS = :STATUS, DTIMPLANTE = TO_DATE(:DTA, 'DD/MM/YYYY'), CODUSREG1 = :CODUSU, DTALTEREGI1 = SYSDATE WHERE IDCOBERTURA = :IDCOBERTURA");
			nativeSql.setNamedParameter("IDCOBERTURA", IDCOBERTURA);
			nativeSql.setNamedParameter("STATUS", STATUS);
			nativeSql.setNamedParameter("DTA", DTA);
			nativeSql.setNamedParameter("CODUSU", CODUSU);
			nativeSql.executeUpdate();

		} catch (Exception e) {
			throw new Exception("Erro ao registrar implantes.", e);
		} finally {
			closeSession();
		}
	}
	
	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void retImplantar(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		BigDecimal IDCOBERTURA = JsonUtils.getBigDecimal(requestBody, "IDCOBERTURA");
		String DTA = JsonUtils.getString(requestBody, "DTA");
		String STATUS = JsonUtils.getString(requestBody, "STATUS");
		BigDecimal CODUSU = JsonUtils.getBigDecimal(requestBody, "CODUSU");

		try {
			openSession();

			nativeSql.resetSqlBuf();
			nativeSql.appendSql("UPDATE BVCOBERTURAS SET STATUS = :STATUS, DTRETIMPLANTE = TO_DATE(:DTA, 'DD/MM/YYYY'), CODUSRET2 = :CODUSU, DTALTERETI2 = SYSDATE WHERE IDCOBERTURA = :IDCOBERTURA");
			nativeSql.setNamedParameter("IDCOBERTURA", IDCOBERTURA);
			nativeSql.setNamedParameter("STATUS", STATUS);
			nativeSql.setNamedParameter("DTA", DTA);
			nativeSql.setNamedParameter("CODUSU", CODUSU);
			nativeSql.executeUpdate();

		} catch (Exception e) {
			throw new Exception("Erro ao retirar Implantes.", e);
		} finally {
			closeSession();
		}
	}
	
	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void iniCobertura(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		BigDecimal IDCOBERTURA = JsonUtils.getBigDecimal(requestBody, "IDCOBERTURA");
		String DTA = JsonUtils.getString(requestBody, "DTA");
		String STATUS = JsonUtils.getString(requestBody, "STATUS");
		BigDecimal CODUSU = JsonUtils.getBigDecimal(requestBody, "CODUSU");

		try {
			openSession();

			nativeSql.resetSqlBuf();
			nativeSql.appendSql("UPDATE BVCOBERTURAS SET STATUS = :STATUS, DTCOBERTURA = TO_DATE(:DTA, 'DD/MM/YYYY'), CODUSCOB3 = :CODUSU, DTALTERCOB3 = SYSDATE WHERE IDCOBERTURA = :IDCOBERTURA");
			nativeSql.setNamedParameter("IDCOBERTURA", IDCOBERTURA);
			nativeSql.setNamedParameter("STATUS", STATUS);
			nativeSql.setNamedParameter("DTA", DTA);
			nativeSql.setNamedParameter("CODUSU", CODUSU);
			nativeSql.executeUpdate();

		} catch (Exception e) {
			throw new Exception("Erro ao iniciar coberturas.", e);
		} finally {
			closeSession();
		}
	}
	
	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void descartarCobertura(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		BigDecimal IDCOBERTURA = JsonUtils.getBigDecimal(requestBody, "IDCOBERTURA");
		BigDecimal CODUSU = JsonUtils.getBigDecimal(requestBody, "CODUSU");

		try {
			openSession();

			nativeSql.resetSqlBuf();
			nativeSql.appendSql("UPDATE BVCOBERTURAS SET STATUS = 'DE', DTCOBERTURA = NULL, CODUSCOB3 = :CODUSU, DTALTERCOB3 = SYSDATE WHERE IDCOBERTURA = :IDCOBERTURA");
			nativeSql.setNamedParameter("IDCOBERTURA", IDCOBERTURA);
			nativeSql.setNamedParameter("CODUSU", CODUSU);
			nativeSql.executeUpdate();

		} catch (Exception e) {
			throw new Exception("Erro ao iniciar descartar coberturas.", e);
		} finally {
			closeSession();
		}
	}

	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void confirmaDiag(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		BigDecimal IDCOBERTURA = JsonUtils.getBigDecimal(requestBody, "IDCOBERTURA");
		String DTA = JsonUtils.getString(requestBody, "DTA");
		BigDecimal CODUSU = JsonUtils.getBigDecimal(requestBody, "CODUSU");
		
		String DGC = JsonUtils.getString(requestBody, "DGC");

		try {
			openSession();

			nativeSql.resetSqlBuf();
			nativeSql.appendSql(
					"S".equals(DGC) ? 
							"UPDATE BVCOBERTURAS SET STATUS = 'PR', DTDGC = TO_DATE(:DTA, 'DD/MM/YYYY'), CODUSDIG4 = :CODUSU, DTALTERDIG4 = SYSDATE WHERE IDCOBERTURA = :IDCOBERTURA" //DGC Positivo
							: "UPDATE BVCOBERTURAS SET STATUS = 'NE', DTDGC = TO_DATE(:DTA, 'DD/MM/YYYY'), DTINCLUSAO = SYSDATE, CODUSDIG4 = :CODUSU, DTALTERDIG4 = SYSDATE WHERE IDCOBERTURA = :IDCOBERTURA" //DGC Negativo
					);
			nativeSql.setNamedParameter("IDCOBERTURA", IDCOBERTURA);
			nativeSql.setNamedParameter("DTA", DTA);
			nativeSql.setNamedParameter("CODUSU", CODUSU);
			nativeSql.executeUpdate();

		} catch (Exception e) {
			throw new Exception("Erro ao confirmar diagnostico.", e);
		} finally {
			closeSession();
		}
	}

	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void confirmaNascimento(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		BigDecimal IDNASCIMENTO = BigDecimal.ZERO;

		BigDecimal IDCOBERTURA = JsonUtils.getBigDecimal(requestBody, "IDCOBERTURA");
		if (IDCOBERTURA == null)
			throw new Exception("Não foi informado o Id da Cobertura, entre em contato com a TI.");

		String SEXO = JsonUtils.getString(requestBody, "SEXO");
		String OCORPRODUTIVA = JsonUtils.getString(requestBody, "OCORPRODUTIVA");
		String NOME = JsonUtils.getString(requestBody, "NOME");
		String TIPOPARTO = JsonUtils.getString(requestBody, "TIPOPARTO");
		BigDecimal IDCATEGORIA = JsonUtils.getBigDecimal(requestBody, "IDCATEGORIA");
		BigDecimal IDRACA = JsonUtils.getBigDecimal(requestBody, "IDRACA");
		BigDecimal IDCOMPRACIAL = JsonUtils.getBigDecimal(requestBody, "IDCOMPRACIAL");
		BigDecimal IDPROPRIETARIO = JsonUtils.getBigDecimal(requestBody, "IDPROPRIETARIO");
		BigDecimal IDCRIADOR = JsonUtils.getBigDecimal(requestBody, "IDCRIADOR");
		BigDecimal IDFAZENDA = JsonUtils.getBigDecimal(requestBody, "IDFAZENDA");
		String STATUS = JsonUtils.getString(requestBody, "STATUS");
		String PROCEDENCIA = JsonUtils.getString(requestBody, "PROCEDENCIA");
		String DTNASC = JsonUtils.getString(requestBody, "DTNASC");
		BigDecimal IDPELAGEM = JsonUtils.getBigDecimal(requestBody, "IDPELAGEM");
		BigDecimal PESONASCIMENTO = JsonUtils.getBigDecimal(requestBody, "PESONASCIMENTO");
		String OBS = JsonUtils.getString(requestBody, "OBS");
		String BOTON = JsonUtils.getString(requestBody, "BOTON");
		BigDecimal IDMAE = JsonUtils.getBigDecimal(requestBody, "IDMAE");
		BigDecimal IDPAI = JsonUtils.getBigDecimal(requestBody, "IDPAI");
		
		BigDecimal CODUSU = JsonUtils.getBigDecimal(requestBody, "CODUSU");

		try {
			openSession();

			nativeSql.resetSqlBuf();
			nativeSql.appendSql("INSERT INTO BVNASCIMENTOANIMAL (IDNASCIMENTOANIMAL, IDCATEGORIA, IDPROPRIETARIO, IDCRIADOR, IDCOMPRACIAL, IDFAZENDA, IDRACA, IDFAZCRIADOR, DTNASC, NOME, SEXO, OCORPRODUTIVA, BOTON, PROCEDENCIA, PESONASCIMENTO, IDPELAGEM, TIPOPARTO, OBS, IDCOBERTURA, STATUS) VALUES((SELECT NVL(MAX(IDNASCIMENTOANIMAL),0)+1 FROM BVNASCIMENTOANIMAL), :IDCATEGORIA, :IDPROPRIETARIO, :IDCRIADOR, :IDCOMPRACIAL, :IDFAZENDA, :IDRACA, 0, TO_DATE(:DTNASC, 'DD/MM/YYYY'), :NOME, :SEXO, :OCORPRODUTIVA, :BOTON, :PROCEDENCIA, :PESONASCIMENTO, :IDPELAGEM, :TIPOPARTO, :OBS, :IDCOBERTURA, :STATUS)");
			nativeSql.setNamedParameter("IDCATEGORIA", IDCATEGORIA);
			nativeSql.setNamedParameter("IDPROPRIETARIO", IDPROPRIETARIO);
			nativeSql.setNamedParameter("IDCRIADOR", IDCRIADOR);
			nativeSql.setNamedParameter("IDCOMPRACIAL", IDCOMPRACIAL);
			nativeSql.setNamedParameter("IDFAZENDA", IDFAZENDA);
			nativeSql.setNamedParameter("IDRACA", IDRACA);
			nativeSql.setNamedParameter("DTNASC", DTNASC);
			nativeSql.setNamedParameter("NOME", NOME);
			nativeSql.setNamedParameter("SEXO", SEXO);
			nativeSql.setNamedParameter("OCORPRODUTIVA", OCORPRODUTIVA);
			nativeSql.setNamedParameter("BOTON", BOTON);
			nativeSql.setNamedParameter("PROCEDENCIA", PROCEDENCIA);
			nativeSql.setNamedParameter("PESONASCIMENTO", PESONASCIMENTO);
			nativeSql.setNamedParameter("IDPELAGEM", IDPELAGEM);
			nativeSql.setNamedParameter("TIPOPARTO", TIPOPARTO);
			nativeSql.setNamedParameter("OBS", OBS);
			nativeSql.setNamedParameter("IDCOBERTURA", IDCOBERTURA);
			nativeSql.setNamedParameter("STATUS", STATUS);

			Boolean hasNascimento = nativeSql.executeUpdate();

			IDNASCIMENTO = getIDNASCIMENTOANIMAL(IDCOBERTURA);

			if (hasNascimento) {
				nativeSql.resetSqlBuf();
				nativeSql.appendSql("INSERT INTO BVANIMAIS (IDANIMAL, NOME, REGISTRO, BOTON, DTNASC, SEXO, CDNORIGEM, CDCORIGEM, IDPELAGEM, IDCATEGORIA, IDPROPRIETARIO, IDCRIADOR, IDCOMPRACIAL, IDRACA, IDFAZENDA, RGN, RGD, SERIE, NROGESTACAO, STATUS, FOTO, PROCEDENCIA, CODUSU, CODUSUALTER, DTCAD, DHALTER, PESONASCIMENTO, IDPAI, IDMAE, IDFAZCRIADOR, TIPOAQUISICAO, IDRATFINBOV, IDNASCIMENTOANIMAL, TIPOPARTO) VALUES((SELECT NVL(MAX(IDANIMAL),0) + 1 FROM BVANIMAIS), :NOME, '', :BOTON, TO_DATE(:DTNASC, 'DD/MM/YYYY'), :SEXO, '', '', :IDPELAGEM, :IDCATEGORIA, :IDPROPRIETARIO, :IDCRIADOR, :IDCOMPRACIAL, :IDRACA, :IDFAZENDA, '', 'P', '', '', 'S', '', :PROCEDENCIA, STP_GET_CODUSULOGADO, STP_GET_CODUSULOGADO, SYSDATE, SYSDATE, :PESONASCIMENTO, :IDPAI, :IDMAE, 0, 'N', NULL, :IDNASCIMENTO, :TIPOPARTO)");
				nativeSql.setNamedParameter("IDMAE", IDMAE);
				nativeSql.setNamedParameter("IDPAI", IDPAI);
				nativeSql.setNamedParameter("IDNASCIMENTO", IDNASCIMENTO);
				nativeSql.setNamedParameter("TIPOPARTO", TIPOPARTO);

				Boolean hasAnimal = nativeSql.executeUpdate();
				
				nativeSql.resetSqlBuf();
				nativeSql.appendSql("MERGE INTO BVTIPOSOCO TGT USING (SELECT NVL((SELECT IDTIPOOCO FROM BVTIPOSOCO WHERE IDTIPOOCO = 1),-1) AS IDTIPOOCO FROM DUAL) SRC ON (TGT.IDTIPOOCO = SRC.IDTIPOOCO) WHEN NOT MATCHED THEN INSERT (IDTIPOOCO, CODUSU, CODUSUALTER, DESCRICAO, ATIVO, DTINCLUSAO, DTALTER) VALUES ('1', '0', '0', 'Nascimento', 'S', SYSDATE, SYSDATE)");
				nativeSql.executeUpdate();
				
				nativeSql.resetSqlBuf();
				nativeSql.appendSql("INSERT INTO BVOCORRENCIA (IDOCOR, IDANIMAL, IDTIPOOCO, CODUSU, CODUSUALTER, DTOCOR, DTINCLUSAO, DTALTER) VALUES ((SELECT NVL(MAX(IDOCOR), 0) + 1 FROM BVOCORRENCIA), (SELECT MAX(IDANIMAL) FROM BVANIMAIS WHERE IDNASCIMENTOANIMAL = :IDNASCIMENTO), '1', :CODUSU, :CODUSU, SYSDATE, SYSDATE, SYSDATE)");
				nativeSql.setNamedParameter("CODUSU", CODUSU);
				nativeSql.setNamedParameter("IDNASCIMENTO", IDNASCIMENTO);
				nativeSql.executeUpdate();

				if (hasAnimal) {
					nativeSql.resetSqlBuf();
					nativeSql.appendSql("UPDATE BVCOBERTURAS SET STATUS = 'NA', IDNASCIMENTO = :IDNASCIMENTO, DTINCLUSAO = SYSDATE, IDANIMALNAS = (SELECT MAX(IDANIMAL) FROM BVANIMAIS WHERE IDNASCIMENTOANIMAL = :IDNASCIMENTO), CODUSNA5 = :CODUSU, DTALTERNA5 = SYSDATE WHERE IDCOBERTURA = :IDCOBERTURA");
					nativeSql.setNamedParameter("CODUSU", CODUSU);
					nativeSql.executeUpdate();
				}

			}

		} catch (Exception e) {
			throw new Exception("Erro ao confirmar nascimento.", e);
		} finally {
			closeSession();
		}
		XMLUtils.addAttributeElement(ctx.getBodyElement(), "IDNASCIMENTO", IDNASCIMENTO);
	}

	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void salvaNascimento(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		BigDecimal IDNASCIMENTO = BigDecimal.ZERO;

		BigDecimal IDCOBERTURA = JsonUtils.getBigDecimal(requestBody, "IDCOBERTURA");
		if (IDCOBERTURA == null)
			throw new Exception("Não foi informado o Id da Cobertura, entre em contato com a TI.");

		String TIPOPARTO = JsonUtils.getString(requestBody, "TIPOPARTO");
		String STATUS = JsonUtils.getString(requestBody, "STATUS");
		String OBS = JsonUtils.getString(requestBody, "OBS");
		BigDecimal CODUSU = JsonUtils.getBigDecimal(requestBody, "CODUSU");

		try {
			openSession();

			nativeSql.resetSqlBuf();
			nativeSql.appendSql("INSERT INTO BVNASCIMENTOANIMAL (IDNASCIMENTOANIMAL, IDCATEGORIA, IDPROPRIETARIO, IDCRIADOR, IDCOMPRACIAL, IDFAZENDA, IDRACA, IDFAZCRIADOR, DTNASC, NOME, SEXO, OCORPRODUTIVA, BOTON, PROCEDENCIA, PESONASCIMENTO, IDPELAGEM, TIPOPARTO, OBS, IDCOBERTURA, STATUS) VALUES((SELECT NVL(MAX(IDNASCIMENTOANIMAL),0)+1 FROM BVNASCIMENTOANIMAL), :IDCATEGORIA, :IDPROPRIETARIO, :IDCRIADOR, :IDCOMPRACIAL, :IDFAZENDA, :IDRACA, 0, :DTNASC, :NOME, :SEXO, :OCORPRODUTIVA, :BOTON, :PROCEDENCIA, :PESONASCIMENTO, :IDPELAGEM, :TIPOPARTO, :OBS, :IDCOBERTURA, :STATUS)");
			nativeSql.setNamedParameter("IDCOBERTURA", IDCOBERTURA);
			nativeSql.setNamedParameter("IDCATEGORIA", BigDecimal.ZERO);
			nativeSql.setNamedParameter("IDPROPRIETARIO", BigDecimal.ZERO);
			nativeSql.setNamedParameter("IDCRIADOR", BigDecimal.ZERO);
			nativeSql.setNamedParameter("IDCOMPRACIAL", BigDecimal.ZERO);
			nativeSql.setNamedParameter("IDFAZENDA", BigDecimal.ZERO);
			nativeSql.setNamedParameter("IDRACA", BigDecimal.ZERO);
			nativeSql.setNamedParameter("DTNASC", null);
			nativeSql.setNamedParameter("NOME", "");
			nativeSql.setNamedParameter("SEXO", "");
			nativeSql.setNamedParameter("OCORPRODUTIVA", "");
			nativeSql.setNamedParameter("BOTON", "");
			nativeSql.setNamedParameter("PROCEDENCIA", "");
			nativeSql.setNamedParameter("PESONASCIMENTO", BigDecimal.ZERO);
			nativeSql.setNamedParameter("IDPELAGEM", BigDecimal.ZERO);
			nativeSql.setNamedParameter("TIPOPARTO", TIPOPARTO);
			nativeSql.setNamedParameter("OBS", OBS);
			nativeSql.setNamedParameter("STATUS", STATUS);

			Boolean hasNascimento = nativeSql.executeUpdate();

			IDNASCIMENTO = getIDNASCIMENTOANIMAL(IDCOBERTURA);

			if (hasNascimento) {
				nativeSql.resetSqlBuf();
				nativeSql.appendSql("UPDATE BVCOBERTURAS SET STATUS = 'NE', IDNASCIMENTO = :IDNASCIMENTO, DTINCLUSAO = SYSDATE, CODUSNA5 = :CODUSU, DTALTERNA5 = SYSDATE WHERE IDCOBERTURA = :IDCOBERTURA");
				nativeSql.setNamedParameter("IDNASCIMENTO", IDNASCIMENTO);
				nativeSql.setNamedParameter("CODUSU", CODUSU);
				nativeSql.executeUpdate();
			}

		} catch (Exception e) {
			throw new Exception("Erro ao salvar nascimento.", e);
		} finally {
			closeSession();
		}
		XMLUtils.addAttributeElement(ctx.getBodyElement(), "IDNASCIMENTO", IDNASCIMENTO);
	}
	
	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void buscaNascimento(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();
		
		JSONObject data = new JSONObject();

		BigDecimal IDCOBERTURA = JsonUtils.getBigDecimal(requestBody, "IDCOBERTURA");
		if (IDCOBERTURA == null)
			throw new Exception("Não foi informado o Id da Cobertura, entre em contato com a TI.");

		try {
			openSession();

			nativeSql.resetSqlBuf();
			nativeSql.appendSql("SELECT IDNASCIMENTOANIMAL, IDCATEGORIA, IDPROPRIETARIO, IDCRIADOR, IDCOMPRACIAL, IDFAZENDA, IDRACA, TO_CHAR(DTNASC, 'DD/MM/YYYY') AS DTNASC, NOME, SEXO, OCORPRODUTIVA, BOTON, PROCEDENCIA, PESONASCIMENTO, IDPELAGEM, OBS, STATUS, TIPOPARTO FROM BVNASCIMENTOANIMAL WHERE IDCOBERTURA = :IDCOBERTURA");
			nativeSql.setNamedParameter("IDCOBERTURA", IDCOBERTURA);

			ResultSet rs = nativeSql.executeQuery();

			if (rs.next()) {
				data.put("IDNASCIMENTOANIMAL", rs.getBigDecimal("IDNASCIMENTOANIMAL"));
				data.put("IDCATEGORIA", rs.getBigDecimal("IDCATEGORIA"));
				data.put("IDPROPRIETARIO", rs.getBigDecimal("IDPROPRIETARIO"));
				data.put("IDCRIADOR", rs.getBigDecimal("IDCRIADOR"));
				data.put("IDCOMPRACIAL", rs.getBigDecimal("IDCOMPRACIAL"));
				data.put("IDFAZENDA", rs.getBigDecimal("IDFAZENDA"));
				data.put("IDRACA", rs.getBigDecimal("IDRACA"));
				data.put("DTNASC", rs.getString("DTNASC"));
				data.put("NOME", rs.getString("NOME"));
				data.put("SEXO", rs.getString("SEXO"));
				data.put("OCORPRODUTIVA", rs.getString("OCORPRODUTIVA"));
				data.put("BOTON", rs.getString("BOTON"));
				data.put("PROCEDENCIA", rs.getString("PROCEDENCIA"));
				data.put("PESONASCIMENTO", rs.getBigDecimal("PESONASCIMENTO"));
				data.put("IDPELAGEM", rs.getBigDecimal("IDPELAGEM"));
				data.put("OBS", rs.getString("OBS"));
				data.put("STATUS", rs.getString("STATUS"));
				data.put("TIPOPARTO", rs.getString("TIPOPARTO"));
			}
			
		} catch (Exception e) {
			throw new Exception("Erro ao buscar nascimento.", e);
		} finally {
			closeSession();
		}
		XMLUtils.addAttributeElement(ctx.getBodyElement(), "NASCIMENTO", data.toString());
	}

	private BigDecimal getIDNASCIMENTOANIMAL(final BigDecimal IDCOBERTURA) throws Exception {
		BigDecimal IDNASCIMENTO = BigDecimal.ZERO;
		try {
			nativeSql.resetSqlBuf();
			nativeSql.setNamedParameter("IDCOBERTURA", IDCOBERTURA);
			ResultSet rs = nativeSql.executeQuery("SELECT IDNASCIMENTOANIMAL FROM BVNASCIMENTOANIMAL WHERE IDCOBERTURA = :IDCOBERTURA");
			if (rs.next()) {
				IDNASCIMENTO = rs.getBigDecimal("IDNASCIMENTOANIMAL");
			}
		} catch (Exception e) {
			throw new Exception("Erro ao buscar id Nascimento.", e);
		}

		return IDNASCIMENTO;
	}
	
	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void insereItemMovAniNota(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		BigDecimal IDMOV = JsonUtils.getBigDecimal(requestBody, "IDMOV");
		BigDecimal NUNOTA = JsonUtils.getBigDecimal(requestBody, "NUNOTA");
		BigDecimal CODPROD = JsonUtils.getBigDecimal(requestBody, "CODPROD");
		BigDecimal QTDNEG = JsonUtils.getBigDecimal(requestBody, "QTDNEG");
		
		final JapeWrapper custoWrapper = JapeFactory.dao("ItemMovimentacoesAnimal");
		
		int SEQUENCIA = 0;
		ResultSet rs = null;

		try {
			openSession();
			
			nativeSql.resetSqlBuf();
			nativeSql.setNamedParameter("IDMOV", IDMOV);
			rs = nativeSql.executeQuery("SELECT NVL(MAX(SEQUENCIA),0)+1 AS SEQUENCIA FROM BVMOVANIMAIS WHERE IDMOV = :IDMOV");
			if (rs.next()) {
				SEQUENCIA = rs.getBigDecimal("SEQUENCIA").intValue();
				
				for(int i=SEQUENCIA;i<=(SEQUENCIA + QTDNEG.intValue() - 1);i++) {
					FluidCreateVO flCrVo = custoWrapper.create();
			        flCrVo.set("SEQUENCIA", (Object) new BigDecimal(i));
			        flCrVo.set("NUNOTA", (Object) NUNOTA);
			        flCrVo.set("IDMOV", (Object) IDMOV);
			        flCrVo.set("CODPROD", (Object) CODPROD);
			        flCrVo.set("IDANIMAL", (Object) null);
			        flCrVo.save();
				}
			}

		} catch (Exception e) {
			throw new Exception("Erro ao inserir itens Movimentações de Animais.", e);
		} finally {
			liberarRecursosConexaoBD(rs);
			closeSession();
		}
	}
	
	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void ocorrenciaAnimal(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		BigDecimal IDMOV = JsonUtils.getBigDecimal(requestBody, "IDMOV");
		BigDecimal NUNOTA = JsonUtils.getBigDecimal(requestBody, "NUNOTA");
		String TIPMOV = JsonUtils.getString(requestBody, "TIPMOV");
		
		String CODUSU = ((AuthenticationInfo) ctx.getAutentication()).getUserID().toString();
		
		ResultSet rs = null;
		
		if (TIPMOV == null)
			throw new Exception("Não foi informado o tipo da movimentação, entre em contato com a TI.");
		
		openSession();
		
		nativeSql.resetSqlBuf();
		nativeSql.setNamedParameter("IDMOV", IDMOV);
		nativeSql.setNamedParameter("NUNOTA", NUNOTA);
		
		if("V".equals(TIPMOV) || "C".equals(TIPMOV)) {
			BigDecimal QTD_ITEMSMOV = BigDecimal.ZERO;
			BigDecimal QTD_ITEMSNOT = BigDecimal.ZERO;
			
			try {
				
				rs = nativeSql.executeQuery("SELECT COUNT(*) AS ITEMSMOV FROM BVMOVANIMAIS WHERE IDMOV = :IDMOV AND NUNOTA = :NUNOTA");
				if (rs.next()) {
					QTD_ITEMSMOV = rs.getBigDecimal("ITEMSMOV");
				}
				
				rs = nativeSql.executeQuery("SELECT COUNT(*) AS ITEMSNOT FROM TGFITE WHERE NUNOTA = :NUNOTA");
				if (rs.next()) {
					QTD_ITEMSNOT = rs.getBigDecimal("ITEMSNOT");
				}
				
				if(QTD_ITEMSMOV.compareTo(QTD_ITEMSNOT) == 0) {
					nativeSql.resetSqlBuf();
					nativeSql.appendSql("UPDATE TGFCAB SET IDMOV = :IDMOV WHERE NUNOTA = :NUNOTA");
					nativeSql.executeUpdate();
					
					nativeSql.resetSqlBuf();
					nativeSql.appendSql("UPDATE BVMOV SET STATUSMOV = 'F' WHERE IDMOV = :IDMOV");
					Boolean success = nativeSql.executeUpdate();
					
					nativeSql.resetSqlBuf();
					nativeSql.appendSql("MERGE INTO BVTIPOSOCO TGT USING (SELECT NVL((SELECT IDTIPOOCO FROM BVTIPOSOCO WHERE IDTIPOOCO = " + ("C".equals(TIPMOV) ? "2" : "3") + "),-1) AS IDTIPOOCO FROM DUAL) SRC ON (TGT.IDTIPOOCO = SRC.IDTIPOOCO) WHEN NOT MATCHED THEN INSERT (IDTIPOOCO, CODUSU, CODUSUALTER, DESCRICAO, ATIVO, DTINCLUSAO, DTALTER) VALUES ('"+ ("C".equals(TIPMOV) ? "2" : "3") + "', '0', '0', '" + ("C".equals(TIPMOV) ? "Compra" : "Venda") + "', 'S', SYSDATE, SYSDATE)");
					nativeSql.executeUpdate();
					
					rs = nativeSql.executeQuery("SELECT BVMOVANIMAIS.IDANIMAL FROM BVMOV, BVMOVANIMAIS WHERE BVMOV.IDMOV = :IDMOV AND BVMOV.NUNOTA = :NUNOTA AND BVMOV.IDMOV = BVMOVANIMAIS.IDMOV");
					while (rs.next()) {
						nativeSql.resetSqlBuf();
						nativeSql.appendSql("INSERT INTO BVOCORRENCIA (IDOCOR, IDANIMAL, IDTIPOOCO, CODUSU, CODUSUALTER, DTOCOR, DTINCLUSAO, DTALTER, NUNOTA) VALUES ((SELECT NVL(MAX(IDOCOR), 0) + 1 FROM BVOCORRENCIA), :IDANIMAL, '" + ("C".equals(TIPMOV) ? "2" : "3") + "', :CODUSU, :CODUSU, SYSDATE, SYSDATE, SYSDATE, :NUNOTA)");
						nativeSql.setNamedParameter("CODUSU", CODUSU);
						nativeSql.setNamedParameter("NUNOTA", NUNOTA);
						nativeSql.setNamedParameter("IDANIMAL", rs.getBigDecimal("IDANIMAL"));
						nativeSql.executeUpdate();
					}
					
					XMLUtils.addAttributeElement(ctx.getBodyElement(), "VC", success);
				} else {
					throw new Exception("Número de animais apontados é diferente da quantidade referido na Nota. "
							+ "Por gentileza, revise os itens de movimentação animal.<br><br>"
							+ " Quantidade animal, id " + IDMOV + ", apontado: " + QTD_ITEMSMOV + "<br>"
							+ " Quantidade produto, nro único Nota " + NUNOTA + ", apontado: " + QTD_ITEMSNOT);
				}
				
			} catch (Exception e) {
				throw new Exception("Erro ao confirmar operação de " + ("V".equals(TIPMOV) ? "Venda." : "Compra."), e);
			} finally {
				closeSession();
				liberarRecursosConexaoBD(rs);
			}
		} else {
			try {
				openSession();
				
				nativeSql.appendSql("MERGE INTO BVTIPOSOCO TGT USING (SELECT NVL((SELECT IDTIPOOCO FROM BVTIPOSOCO WHERE IDTIPOOCO = '4'),-1) AS IDTIPOOCO FROM DUAL) SRC ON (TGT.IDTIPOOCO = SRC.IDTIPOOCO) WHEN NOT MATCHED THEN INSERT (IDTIPOOCO, CODUSU, CODUSUALTER, DESCRICAO, ATIVO, DTINCLUSAO, DTALTER) VALUES ('4', '0', '0', 'Transferencia', 'S', SYSDATE, SYSDATE)");
				nativeSql.executeUpdate();
				
				rs = nativeSql.executeQuery("SELECT BVMOVANIMAIS.IDANIMAL, BVMOV.IDFAZENDA, BVMOV.IDFAZDESTINO FROM BVMOV, BVMOVANIMAIS WHERE BVMOV.IDMOV = :IDMOV AND BVMOV.NUNOTA = :NUNOTA AND BVMOV.IDMOV = BVMOVANIMAIS.IDMOV");
				while (rs.next()) {
					BigDecimal IDANIMAL = rs.getBigDecimal("IDANIMAL");
					BigDecimal IDFAZENDA = rs.getBigDecimal("IDFAZENDA");
					BigDecimal IDFAZDESTINO = rs.getBigDecimal("IDFAZDESTINO");
							
					nativeSql.resetSqlBuf();
					nativeSql.appendSql("UPDATE BVANIMAIS SET IDFAZENDA = :IDFAZDESTINO WHERE IDANIMAL = :IDANIMAL");
					nativeSql.setNamedParameter("IDANIMAL", IDANIMAL);
					nativeSql.setNamedParameter("IDFAZDESTINO", IDFAZDESTINO);
					Boolean successUPDATE = nativeSql.executeUpdate();
					
					if(successUPDATE) {
						nativeSql.resetSqlBuf();
						nativeSql.appendSql("INSERT INTO BVOCORRENCIA (IDOCOR, IDANIMAL, IDTIPOOCO, CODUSU, CODUSUALTER, DTOCOR, DTINCLUSAO, DTALTER, IDFAZENDA, IDFAZDESTINO, NUNOTA) VALUES ((SELECT NVL(MAX(IDOCOR), 0) + 1 FROM BVOCORRENCIA), :IDANIMAL, '4', :CODUSU, :CODUSU, SYSDATE, SYSDATE, SYSDATE, :IDFAZENDA, :IDFAZDESTINO, :NUNOTA)");
						nativeSql.setNamedParameter("CODUSU", CODUSU);
						nativeSql.setNamedParameter("IDANIMAL", IDANIMAL);
						nativeSql.setNamedParameter("IDFAZENDA", IDFAZENDA);
						nativeSql.setNamedParameter("NUNOTA", NUNOTA);
						nativeSql.executeUpdate();
					}
				}
				
				nativeSql.resetSqlBuf();
				nativeSql.appendSql("UPDATE BVMOV SET STATUSMOV = 'F' WHERE IDMOV = :IDMOV");
				Boolean success = nativeSql.executeUpdate();
				
				XMLUtils.addAttributeElement(ctx.getBodyElement(), "T", success);
				
			} catch (Exception e) {
				throw new Exception("Erro ao confirmar operação de Transferencia.", e);
			} finally {
				closeSession();
				liberarRecursosConexaoBD(rs);
			}
		}
	}

	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void deleteItensMovAni(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		BigDecimal IDMOV = JsonUtils.getBigDecimal(requestBody, "IDMOV");
		
		try {
			openSession();
			
			nativeSql.resetSqlBuf();
			nativeSql.appendSql("DELETE BVMOVANIMAIS WHERE IDMOV = :IDMOV");
			nativeSql.setNamedParameter("IDMOV", IDMOV);
			nativeSql.executeUpdate();
			
		} catch (Exception e) {
			throw new Exception("Erro ao deletar itens de movimentação animal de id " + IDMOV + ".", e);
		} finally {
			closeSession();
		}
	}
	
	/**
	 * @throws Exception
	 * @ejb.interface-method tview-tipe="remote"
	 * @ejb.transaction type="Required"
	 */
	public void desativarBrinco(ServiceContext ctx) throws Exception {
		final JsonObject requestBody = ctx.getJsonRequestBody();

		String NUBRINCO = JsonUtils.getString(requestBody, "NUBRINCO");
		
		try {
			openSession();
			
			nativeSql.resetSqlBuf();
			nativeSql.appendSql("UPDATE BVBRINCO SET ATIVO = 'N', DTINATIVO = SYSDATE WHERE NUBRINCO = :NUBRINCO");
			nativeSql.setNamedParameter("NUBRINCO", NUBRINCO);
			nativeSql.executeUpdate();
			
		} catch (Exception e) {
			throw new Exception("Erro ao desativar Brinco de NUBRINCO " + NUBRINCO + ".", e);
		} finally {
			closeSession();
		}
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
