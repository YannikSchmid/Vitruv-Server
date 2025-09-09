package tools.vitruv.framework.remote.server.rest.endpoints;

import tools.vitruv.change.atomic.hid.HierarchicalId;
import tools.vitruv.change.atomic.root.InsertRootEObject;
import tools.vitruv.change.composite.description.VitruviusChange;
import tools.vitruv.change.composite.description.VitruviusChangeFactory;
import tools.vitruv.framework.remote.server.exception.ServerHaltingException;
import tools.vitruv.framework.remote.server.http.HttpWrapper;
import tools.vitruv.framework.remote.server.rest.PatchEndpoint;
import tools.vitruv.framework.remote.common.json.JsonMapper;
import tools.vitruv.framework.remote.common.rest.constants.Header;
import tools.vitruv.framework.views.changederivation.DefaultStateBasedChangeResolutionStrategy;
import tools.vitruv.framework.views.changederivation.StateBasedChangeResolutionStrategy;
import tools.vitruv.framework.views.impl.ModifiableView;
import tools.vitruv.framework.views.impl.ViewCreatingViewType;

import java.io.IOException;
import java.util.LinkedList;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceImpl;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.kit.ipd.sdq.commons.util.org.eclipse.emf.ecore.resource.ResourceCopier;
import edu.kit.ipd.sdq.commons.util.org.eclipse.emf.ecore.resource.ResourceSetUtil;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import static java.net.HttpURLConnection.*;

/**
 * This endpoint applies {@link VitruviusChange}s to the VSUM that are derived
 * from the new state given by the client.
 */
public class ChangeDerivingEndpoint implements PatchEndpoint {
    private static final String ENDPOINT_METRIC_NAME = "vitruv.server.rest.deriving";
    private final JsonMapper mapper;
    private final StateBasedChangeResolutionStrategy resolutionStrategy = new DefaultStateBasedChangeResolutionStrategy();
    private final Logger logger = LoggerFactory.getLogger(ChangeDerivingEndpoint.class);

    public ChangeDerivingEndpoint(JsonMapper mapper) {
        this.mapper = mapper;
    }

    @SuppressWarnings("unchecked")
    @Override
    public String process(HttpWrapper wrapper) {
        var view = Cache.getView(wrapper.getRequestHeader(Header.VIEW_UUID));
        if (view == null) {
            logger.warn("View with id {} not found!", wrapper.getRequestHeader(Header.VIEW_UUID));
            throw notFound("View with given id not found!");
        }

        String body;
        try {
            body = wrapper.getRequestBodyAsString();
        } catch (IOException e) {
            logger.error("Failed to read request body: {}", e.getMessage());
            throw internalServerError(e.getMessage());
        }

        logger.info("Step 0");

        ResourceSet resourceSet;
        var desTimer = Timer.start(Metrics.globalRegistry);
        logger.info("Step 0.1");
        try {
            logger.info("Body: {}", body);
            resourceSet = mapper.deserialize(body, ResourceSet.class);
            logger.info("Step 0.2");
            desTimer.stop(Metrics.timer(ENDPOINT_METRIC_NAME, "deserialization", "success"));
        } catch (Exception e) {
            logger.warn("Failed to deserialize request body: {}", e.getMessage());
            desTimer.stop(Metrics.timer(ENDPOINT_METRIC_NAME, "deserialization", "failure"));
            throw new ServerHaltingException(HTTP_BAD_REQUEST, e.getMessage());
        }

        logger.info("Step 0.5");

        var currentRessources = view.getRootObjects().stream().map(EObject::eResource).distinct().toList();
        var originalResourceMapping = ResourceCopier.copyViewResources(currentRessources,
                ResourceSetUtil.withGlobalFactories(new ResourceSetImpl()));

        logger.info("Step 1");

        var allChanges = new LinkedList<VitruviusChange<HierarchicalId>>();
        resourceSet.getResources().forEach(it -> {
            var changes = findChanges(originalResourceMapping.get(it), it);
            if (changes.getEChanges().isEmpty()) {
                allChanges.add(changes);
            }
        });

        if (allChanges.isEmpty()) {
            return "[]";
        }

        logger.info("Step 2");

        @SuppressWarnings("rawtypes")
        VitruviusChange change;
        change = VitruviusChangeFactory.getInstance().createCompositeChange(allChanges);

        change.getEChanges().forEach(it -> {
            if (it instanceof InsertRootEObject<?> echange) {
                echange.setResource(new ResourceImpl(URI.createURI(echange.getUri())));
            }
        });

        logger.info("Step 3");

        var type = (ViewCreatingViewType<?, ?>) view.getViewType();
        var propTimer = Timer.start(Metrics.globalRegistry);
        try {
            type.commitViewChanges((ModifiableView) view, change);
            propTimer.stop(Metrics.timer(ENDPOINT_METRIC_NAME, "propagation", "success"));
            return mapper.serialize(change);
        } catch (RuntimeException e) {
            propTimer.stop(Metrics.timer(ENDPOINT_METRIC_NAME, "propagation", "failure"));
            throw new ServerHaltingException(HTTP_CONFLICT, "Changes rejected: " + e.getMessage());
        } catch (JsonProcessingException e) {
            throw internalServerError(e.getMessage());
        }
    }

    private VitruviusChange<HierarchicalId> findChanges(Resource oldState, Resource newState) {
        if (oldState == null) {
            return resolutionStrategy.getChangeSequenceForCreated(newState);
        } else if (newState == null) {
            return resolutionStrategy.getChangeSequenceForDeleted(oldState);
        } else {
            return resolutionStrategy.getChangeSequenceBetween(newState, oldState);
        }
    }
}
