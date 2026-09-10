package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.monitor.DashboardService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.util.List;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(DashboardController.class)
class DashboardControllerTest {
    @Autowired MockMvc mvc;
    @MockitoBean DashboardService dashboard;
    @Test @WithMockUser(roles = "DEVELOPER")
    void developerCanReadAndMissingValuesStayNull() throws Exception {
        given(dashboard.snapshot()).willReturn(new DashboardService.Dashboard(null, true, null, null,
                null, null, List.of(), List.of(), List.of()));
        mvc.perform(get("/api/dashboard")).andExpect(status().isOk())
                .andExpect(jsonPath("$.stale").value(true)).andExpect(jsonPath("$.incomingPerSec").isEmpty());
    }
    @Test void anonymousCannotRead() throws Exception {
        mvc.perform(get("/api/dashboard")).andExpect(status().isUnauthorized());
    }
}
