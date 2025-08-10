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

import com.fasterxml.jackson.core.JsonProcessingException;

import edu.kit.ipd.sdq.commons.util.org.eclipse.emf.ecore.resource.ResourceCopier;
import edu.kit.ipd.sdq.commons.util.org.eclipse.emf.ecore.resource.ResourceSetUtil;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

import static java.net.HttpURLConnection.*;

/**
 * This endpoint applies given {@link VitruviusChange}s to the VSUM.
 */
public class ChangeDerivingEndpoint implements PatchEndpoint {
    private static final String ENDPOINT_METRIC_NAME = "vitruv.server.rest.deriving";
    private final JsonMapper mapper;
    private final StateBasedChangeResolutionStrategy resolutionStrategy;

    public ChangeDerivingEndpoint(JsonMapper mapper, StateBasedChangeResolutionStrategy resolutionStrategy) {
        this.mapper = mapper;
        this.resolutionStrategy = resolutionStrategy;
    }

    @SuppressWarnings("unchecked")
    @Override
    public String process(HttpWrapper wrapper) {
        var view = Cache.getView(wrapper.getRequestHeader(Header.VIEW_UUID));
        if (view == null) {
            throw notFound("View with given id not found!");
        }

        String body;
        try {
            body = wrapper.getRequestBodyAsString();
        } catch (IOException e) {
            throw internalServerError(e.getMessage());
        }

        ResourceSet resourceSet;
        var desTimer = Timer.start(Metrics.globalRegistry);
        try {
            resourceSet = mapper.deserialize(body, ResourceSet.class);
            desTimer.stop(Metrics.timer(ENDPOINT_METRIC_NAME, "deserialization", "success"));
        } catch (JsonProcessingException e) {
            desTimer.stop(Metrics.timer(ENDPOINT_METRIC_NAME, "deserialization", "failure"));
            throw new ServerHaltingException(HTTP_BAD_REQUEST, e.getMessage());
        }

        var currentRessources = view.getRootObjects().stream().map(EObject::eResource).distinct().toList();
        var originalResourceMapping = ResourceCopier.copyViewResources(currentRessources,
                ResourceSetUtil.withGlobalFactories(new ResourceSetImpl()));

        var allChanges = new LinkedList<VitruviusChange<HierarchicalId>>();
        resourceSet.getResources().forEach(it -> {
            var changes = findChanges(originalResourceMapping.get(it), it);
            if (changes.getEChanges().isEmpty()) {
                allChanges.add(changes);
            }
        });

        @SuppressWarnings("rawtypes")
        VitruviusChange change;
        change = VitruviusChangeFactory.getInstance().createCompositeChange(allChanges);

        change.getEChanges().forEach(it -> {
            if (it instanceof InsertRootEObject<?> echange) {
                echange.setResource(new ResourceImpl(URI.createURI(echange.getUri())));
            }
        });

        var type = (ViewCreatingViewType<?, ?>) view.getViewType();
        var propTimer = Timer.start(Metrics.globalRegistry);
        try {
            type.commitViewChanges((ModifiableView) view, change);
            propTimer.stop(Metrics.timer(ENDPOINT_METRIC_NAME, "propagation", "success"));
        } catch (RuntimeException e) {
            propTimer.stop(Metrics.timer(ENDPOINT_METRIC_NAME, "propagation", "failure"));
            throw new ServerHaltingException(HTTP_CONFLICT, "Changes rejected: " + e.getMessage());
        }
        return null;
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
