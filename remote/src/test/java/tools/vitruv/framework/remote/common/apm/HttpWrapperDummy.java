package tools.vitruv.framework.remote.common.apm;

import java.io.IOException;
import java.util.Map;

import tools.vitruv.framework.remote.server.http.HttpWrapper;

public class HttpWrapperDummy implements HttpWrapper {

    private Map<String, String> requestHeaders;
    private String requestBody;

    public HttpWrapperDummy(Map<String, String> requestHeaders, String requestBody) {
        this.requestHeaders = requestHeaders;
        this.requestBody = requestBody;
    }

    @Override
    public String getRequestHeader(String header) {
        return this.requestHeaders.get(header);
    }

    @Override
    public String getRequestBodyAsString() throws IOException {
        return this.requestBody;
    }

    @Override
    public void addResponseHeader(String header, String value) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addResponseHeader'");
    }

    @Override
    public void setContentType(String type) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'setContentType'");
    }

    @Override
    public void sendResponse(int responseCode) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'sendResponse'");
    }

    @Override
    public void sendResponse(int responseCode, byte[] body) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'sendResponse'");
    }

}
