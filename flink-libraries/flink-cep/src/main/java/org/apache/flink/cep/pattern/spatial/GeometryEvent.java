package org.apache.flink.cep.pattern.spatial;

import org.locationtech.jts.geom.Geometry;

import java.util.Optional;

/** Base class for all events with geometry info. */
public abstract class GeometryEvent {

    private Optional<Geometry> geometry;

    public GeometryEvent() {
        this.geometry = Optional.empty();
    }

    public GeometryEvent(Geometry geometry) {
        this.geometry = Optional.of(geometry);
    }

    public boolean hasGeometry() {
        return this.geometry.isPresent();
    }

    public Geometry getGeometry() throws Exception {
        if (!this.hasGeometry()) {
            throw new Exception("No geometry event present");
        }
        return this.geometry.get();
    }
}
